# 프론트 정적 서버 — nginx-unprivileged(비루트 uid 101, 8080)로 k3s restricted PSA 충족.
# 라우팅: k3s에선 Traefik Ingress가 /api를 가로챔. 로컬 compose 게이트웨이를 위해 proxy location은 유지
#         (k3s에선 Ingress가 먼저 가로채 이 proxy를 안 탐 → 무해).
# 1.27 라인은 더 굽지 않아 alpine 3.21 에 멈춰 있다. 1.31 은 alpine 3.24 이고 계속
# 다시 굽히므로, --pull 과 함께 쓰면 빌드할 때마다 최신 OS 패치를 받는다.
FROM nginxinc/nginx-unprivileged:1.31-alpine

# 업스트림 이미지가 아직 담지 않은 OS 패치를 빌드 시점에 받는다. --pull 로 최신 태그를 받아도
#   그 안의 패키지가 이미지를 구운 시점에 멈춰 있어, 그 뒤에 나온 수정은 들어오지 않는다.
#   2026-08-27: 이 태그의 alpine 3.24.1 이 openssl 3.5.7-r0 이고 CVE-2026-14456(QUIC 서버
#   메모리 무한 증가로 인한 DoS)이 3.5.8-r0 에서 고쳐져, scan 단계가 이미지를 막았다.
#   패키지를 하나씩 적지 않는 이유는 다음 CVE 가 다른 패키지에서 나기 때문이다.
# 기반 이미지가 uid 101 로 돌아 설치 동안만 root 로 올라간다.
#
# 엔트리포인트 스크립트 중 IPv6 listen 을 켜는 것(10-listen-on-ipv6-by-default.sh)을 뺀다.
#   conf.d 에 default.conf 가 이미 있으면 이 스크립트가 패키지 원본과 비교하려고 `apk manifest nginx` 를
#   부르는데, apk 3 은 이때 저장소 색인(dl-cdn.alpinelinux.org)을 받으러 나간다. 인터넷 egress 가 막힌
#   파드에서는 그 조회가 끝날 때까지 nginx 가 포트를 열지 않는다(로컬 재현: 정상 1초 · DNS 실패 10초 ·
#   연결 거부 42초). 그동안 liveness 가 실패해 컨테이너가 죽고, 다시 떠도 같은 자리에서 멈춘다.
#   conf.d 는 emptyDir 이라 같은 파드 안에서 컨테이너가 재시작하면(노드 재부팅 등) 이전 실행이 쓴
#   default.conf 가 남아 있어 매번 이 경로를 탄다. 새 파드는 conf.d 가 비어 있어 이 경로를 건너뛴다.
# 지워도 잃는 것이 없다 — 이 이미지의 default.conf 는 템플릿에서 만들어져 원본과 체크섬이 달라,
#   이 스크립트는 조회만 하고 설정을 바꾸지 않은 채 끝난다. listen 은 템플릿이 정한다.
# egress 를 여는 것으로 풀지 않는다 — 인터넷에 노출된 파드에 밖으로 나가는 길을 만들게 되고,
#   재시작 속도가 외부 CDN 상태에 묶인다.
USER root
RUN apk --no-cache upgrade \
 && rm -f /docker-entrypoint.d/10-listen-on-ipv6-by-default.sh
USER 101

# 설정을 템플릿으로 넣는다. 기반 이미지의 엔트리포인트가 뜰 때 /etc/nginx/templates/*.template 을
#   읽어 환경변수를 채우고 그 결과를 /etc/nginx/conf.d/ 에 쓴다.
#   resolver 주소가 환경마다 달라서(k3s·EKS·compose) 이미지에 리터럴로 둘 수 없다.
# ★ 렌더 결과가 /etc/nginx/conf.d/ 에 쓰이는데 그 경로는 readOnlyRootFilesystem 아래다.
#   배포 쪽에서 그 경로에 쓰기 가능한 볼륨을 붙여야 기동한다(cgv-infra 의 frontend 차트 값).
COPY default.conf.template /etc/nginx/templates/default.conf.template
COPY index.html style.css og.png /usr/share/nginx/html/
COPY js/ /usr/share/nginx/html/js/
EXPOSE 8080
