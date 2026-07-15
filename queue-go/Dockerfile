# queue-go — multi-stage. distroless static(작고 비루트).
FROM golang:1.26 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /queue-go .

# :nonroot 태그 = uid 65532 실행(필수 1) — runAsNonRoot 정책과 짝. 무태그는 root였음(주석-실체 불일치 교정).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /queue-go /queue-go
EXPOSE 8090 9091
ENTRYPOINT ["/queue-go"]
