# syntax=docker/dockerfile:1
# queue-go — multi-stage. distroless static(작고 비루트).
FROM golang:1.26 AS build
WORKDIR /app
COPY go.mod go.sum ./
# --mount=type=cache: 내려받은 모듈을 레이어가 아니라 빌더가 따로 들고 있는 저장소에 둔다.
#   레이어 캐시만 있으면 go.mod 가 바뀔 때마다 전부 다시 받는다.
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# go-build 는 컴파일 결과 캐시다. 소스가 바뀌어도 안 바뀐 패키지는 다시 컴파일하지 않는다.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o /queue-go .

# :nonroot 태그 = uid 65532 실행(필수 1) — runAsNonRoot 정책과 짝. 무태그는 root였음(주석-실체 불일치 교정).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /queue-go /queue-go
EXPOSE 8090 9091
ENTRYPOINT ["/queue-go"]
