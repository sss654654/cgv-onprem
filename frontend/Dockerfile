# 프론트 정적 서버 — nginx-unprivileged(비루트 uid 101, 8080)로 k3s restricted PSA 충족.
# 라우팅: k3s에선 Traefik Ingress가 /api를 가로챔. 로컬 compose 게이트웨이를 위해 proxy location은 유지
#         (k3s에선 Ingress가 먼저 가로채 이 proxy를 안 탐 → 무해).
# 1.27 라인은 더 굽지 않아 alpine 3.21 에 멈춰 있다. 1.31 은 alpine 3.24 이고 계속
# 다시 굽히므로, --pull 과 함께 쓰면 빌드할 때마다 최신 OS 패치를 받는다.
FROM nginxinc/nginx-unprivileged:1.31-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html style.css og.png /usr/share/nginx/html/
COPY js/ /usr/share/nginx/html/js/
EXPOSE 8080
