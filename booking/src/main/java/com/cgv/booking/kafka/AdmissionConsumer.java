package com.cgv.booking.kafka;

import com.cgv.booking.redis.AdmittedService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

// admissions 소비(queue→booking): "u3 입장했다" → 입장 인증 발급.
// GroupID=booking — 브로커가 이 그룹 offset 기억(죽어도 이어읽기).
// at-least-once라 중복 가능 → 인증 발급은 멱등이라 무해.
@Component
public class AdmissionConsumer {
    private static final Logger log = LoggerFactory.getLogger(AdmissionConsumer.class);
    private final AdmittedService admitted;
    private final ObjectMapper mapper;
    private final MeterRegistry meterRegistry;
    private final Timer lag;

    public AdmissionConsumer(AdmittedService admitted, ObjectMapper mapper, MeterRegistry meterRegistry) {
        this.admitted = admitted;
        this.mapper = mapper;
        this.meterRegistry = meterRegistry;
        // booking_admission_lag_seconds — queue가 승격을 발행한 시각부터 여기서 인증이 생긴 시각까지.
        // 이 값이 있어야 "정원 30인데 실제 동시 예매가 20"의 원인에서 전달 지연을 지울 수 있다.
        // 두 서비스의 발행/소비 건수는 각각 지표로 나오지만, 한 건이 건너오는 데 걸린 시간은
        //   어느 지표에도 없다.
        // 10초까지는 http.server.requests와 같은 눈금이고, 그 위는 이 지표에만 있다.
        // 상한을 10초에서 60초로 넓힌 이유 — 2026-08-21 정원 500 부하에서 p50·p99가 모두
        //   10초(당시 최상단 버킷)에 붙었다. 최상단에 몰리면 분위수가 그 경계값으로 나와,
        //   실제가 10초인지 40초인지 구분이 안 되는 채로 화면에는 "10초"가 뜬다.
        //   같은 판에서 /api/screenings 의 403이 200의 2.4배였다(승격 9.0/초 · 200 9.48/초 ·
        //   403 22.71/초). 자리를 받고 인증을 기다리는 동안 게이트에 막혀 재시도한 것이고,
        //   그 대기 시간이 이 지표다.
        // 60초를 상한으로 두는 근거는 세션 타임아웃(SESSION_TIMEOUT=60)이다. 전달이 그보다
        //   길어지면 그 사용자는 이미 회수되므로, 그 위를 더 나눠도 판단이 달라지지 않는다.
        // 반대 방향과 비교하면 원인 범위가 좁아진다. 같은 판에서 queue_completed_lag_seconds
        //   (확정 발행 → 자리 반환 처리)는 p50 0.032초 · p99 0.080초였다. 같은 Kafka 를 쓰는데
        //   방향에 따라 자릿수가 다르므로, 느린 것은 브로커가 아니라 admissions 경로 쪽이다.
        this.lag = Timer.builder("booking.admission.lag")
                .description("승격 발행(queue) → 입장 인증 발급(booking) 지연")
                .serviceLevelObjectives(
                        Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(10),
                        Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
                        Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofMillis(2500), Duration.ofSeconds(5), Duration.ofSeconds(10),
                        Duration.ofSeconds(20), Duration.ofSeconds(30), Duration.ofSeconds(60))
                .register(meterRegistry);
    }

    // booking_admissions_total{result=...} — 소비 종착점별 카운트.
    // skipped(파싱 실패·필드 누락)를 세지 않으면 "입장했는데 좌석선택 403"의 건수를 사후에 셀 수 없다.
    private void count(String result) {
        meterRegistry.counter("booking.admissions", "result", result).increment();
    }

    // 배치 소비 — poll이 가져온 레코드 묶음을 한 번에 처리한다(팩토리는 KafkaConfig의 배치 전용).
    //
    // 건별 소비(레코드당 호출 1회 + Redis 왕복 1회)는 소비 속도의 상한이 초당 약 28건이었고,
    // 예매 오픈 순간 admissions 수백 건이 몰리면 뒤쪽 레코드의 인증이 최대 10초 밀렸다
    // (2026-08-29 사용자 10,000 부하: 인증 지연 p99 9.94초, 발행·브로커·건별 처리 자체는 전부 정상).
    // 묶음 처리로 호출과 Redis 왕복이 묶음당 1회가 된다 — AdmittedService.addAll(파이프라인)과 짝.
    //
    // 파싱 실패와 처리 실패를 구분한다.
    //   파싱 실패·필드 누락 = 포이즌 메시지 — 재시도해도 똑같이 실패 → 카운터+ERROR 후 스킵.
    //     배치 안에서 걸러내므로 포이즌 하나가 정상 레코드 묶음을 재시도로 끌고 가지 않는다.
    //   인증 발급 실패(Redis 순단) = 예외 전파 → 배치 전체 재시도(오프셋 미커밋).
    //     발급은 멱등(TTL 재설정)이라 이미 성공한 레코드가 다시 처리돼도 무해하다.
    // publishedAts = 카프카 레코드 타임스탬프. 기본 CreateTime이라 발행 측(queue) 파드의 시계로 찍힌다.
    //   두 노드의 시계 차이가 그대로 오차로 섞인다(NTP 동기 기준 수 ms). 시계가 역전되면
    //   음수가 나오는데, 그건 지연이 아니라 잡음이라 버린다.
    // 지연은 addAll이 돌아온 뒤에 잰다 — "이 사용자가 좌석 선택을 통과할 수 있게 된 시점"까지가
    //   재려는 구간이라, 배치의 모든 레코드가 같은 완료 시각을 쓴다.
    @KafkaListener(topics = "admissions", containerFactory = "batchKafkaListenerContainerFactory")
    // groupId는 application.yml(consumer.group-id: booking) 단일 소스
    public void onAdmissions(List<String> messages,
                             @Header(KafkaHeaders.RECEIVED_TIMESTAMP) List<Long> publishedAts) {
        List<AdmittedService.Admission> valid = new ArrayList<>(messages.size());
        List<Long> validPublishedAts = new ArrayList<>(messages.size());

        for (int i = 0; i < messages.size(); i++) {
            String message = messages.get(i);
            QueueEvent e;
            try {
                e = mapper.readValue(message, QueueEvent.class);
            } catch (Exception ex) {
                count("parse_error");
                log.error("admissions 파싱 실패(스킵 — 이 사용자는 재입장 전까지 좌석선택 403): {} ({})", message, ex.getMessage());
                continue;
            }
            if (e.requestId() == null || e.movieId() == null) {
                count("missing_field");
                log.error("admissions 필수 필드 누락(스킵 — 이 사용자는 재입장 전까지 좌석선택 403): {}", message);
                continue;
            }
            valid.add(new AdmittedService.Admission(e.movieId(), e.requestId()));
            validPublishedAts.add(publishedAts.get(i));
        }

        if (!valid.isEmpty()) {
            admitted.addAll(valid);   // 멱등. 실패 시 throw → 배치 재시도
        }

        long now = System.currentTimeMillis();
        for (int i = 0; i < valid.size(); i++) {
            count("ok");
            long elapsed = now - validPublishedAts.get(i);
            if (elapsed >= 0) {
                lag.record(elapsed, TimeUnit.MILLISECONDS);
            }
        }
        // 건별 info 로그는 두지 않는다 — 오픈 순간 초당 수백 줄이 되고, 건별 내용은 트레이스에 있다.
        log.info("입장 인증 추가: batch={} skipped={}", valid.size(), messages.size() - valid.size());
    }
}
