-- V1 — 테이블 다섯. 엔티티(com.cgv.booking.domain)와 같아야 한다. 기동할 때 Hibernate(ddl-auto=validate)가 맞춰 본다.
--
-- Flyway 도입 전에 Hibernate 가 dev 에 만든 스키마를 그대로 옮겨 적었다(컬럼 · 타입 · 인덱스 이름).
--   인덱스 이름이 Hibernate 가 지은 이름 그대로인 이유: 그 DB 는 이 파일을 적용하지 않고 V1 으로 기록만 한다
--   (application.yml 의 baseline-on-migrate). 이름이 같아야 이후 마이그레이션이 두 DB 에서 같은 문장으로 돈다.
--   문자셋만 다르다 — 새로 만드는 DB 는 utf8mb4, Hibernate 가 만든 기존 테이블은 utf8mb3 로 남는다.
--
-- MySQL 은 DDL 을 트랜잭션으로 되돌리지 못한다. 이 파일은 빈 DB 에서 한 번 도는 초기 생성이라 문장 다섯을
--   한 파일에 둔다. 이후 변경은 파일 하나에 DDL 하나로 나눈다 — 중간에 실패하면 반쯤 바뀐 채 남는다.

CREATE TABLE movies (
  id           VARCHAR(255) NOT NULL,
  broadcast_at DATETIME(6)  NULL,
  title        VARCHAR(255) NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE screenings (
  id           VARCHAR(255) NOT NULL,
  branch       VARCHAR(255) NULL,
  movie_id     VARCHAR(255) NULL,
  screen_no    INT          NOT NULL,
  total_seats  INT          NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 좌석 구조. 판매 여부는 여기 없고 booking_seats 에 행이 있는지로 판정한다.
CREATE TABLE seats (
  id           BIGINT       NOT NULL AUTO_INCREMENT,
  screening_id VARCHAR(255) NULL,
  seat_col     INT          NULL,
  seat_no      VARCHAR(255) NULL,
  seat_row     INT          NULL,
  PRIMARY KEY (id),
  UNIQUE KEY UK4cq97rboosgcboleo1kvaywla (screening_id, seat_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 멱등키 UNIQUE — 같은 결제가 두 번 오면 두 번째가 여기서 막히고 먼저 성공한 예매를 돌려준다.
CREATE TABLE bookings (
  id              VARCHAR(255) NOT NULL,
  created_at      DATETIME(6)  NULL,
  idempotency_key VARCHAR(255) NULL,
  price           INT          NOT NULL,
  screening_id    VARCHAR(255) NULL,
  user_id         VARCHAR(255) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY UKqc7ays8yeraglxyak4mq5kb99 (idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- (회차, 좌석) UNIQUE — 같은 좌석을 두 예매가 동시에 확정하면 뒤의 것이 여기서 막힌다.
CREATE TABLE booking_seats (
  id           BIGINT       NOT NULL AUTO_INCREMENT,
  booking_id   VARCHAR(255) NULL,
  screening_id VARCHAR(255) NULL,
  seat_no      VARCHAR(255) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY UKfe7gjwo9cg52ueg8hg1mxj85v (screening_id, seat_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
