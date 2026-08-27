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
USER root
RUN apk --no-cache upgrade
USER 101

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html style.css og.png /usr/share/nginx/html/
COPY js/ /usr/share/nginx/html/js/
EXPOSE 8080
