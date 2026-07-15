# 프론트 정적 서버 — nginx-unprivileged(비루트 uid 101, 8080)로 k3s restricted PSA 충족.
# 라우팅: k3s에선 Traefik Ingress가 /api를 가로챔. 로컬 compose 게이트웨이를 위해 proxy location은 유지
#         (k3s에선 Ingress가 먼저 가로채 이 proxy를 안 탐 → 무해).
FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html style.css app.js /usr/share/nginx/html/
EXPOSE 8080
