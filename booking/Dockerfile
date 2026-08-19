# syntax=docker/dockerfile:1
# booking(Java/Spring) 멀티스테이지 — Maven 빌드 → JRE 런타임.
# 풋프린트 주의: JVM은 무겁다 → 컨테이너 인식 옵션으로 힙 제한. 2-2에서 GraalVM/AppCDS 검토.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
# 빌드 JVM 힙 상한. 아래 runtime 스테이지의 JAVA_OPTS는 그 스테이지 안에서만 유효해
#   빌드하는 JVM은 그 값을 못 본다. 안 주면 JVM이 cgroup이 보고하는 메모리의 25%를
#   힙으로 잡는데, 러너가 도는 VM 상한(6GB) 기준이라 1.5GB까지 커진다.
ENV MAVEN_OPTS="-Xmx768m"
COPY pom.xml .
# --mount=type=cache: 내려받은 라이브러리를 레이어가 아니라 빌더가 따로 들고 있는 저장소에 둔다.
#   레이어 캐시만 있으면 pom.xml 이 한 글자만 바뀌어도 아래가 전부 무효가 되어 처음부터 다시
#   받는다. 이 마운트는 그때도 남아 있어 바뀐 것만 받는다.
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
# 와일드카드로 받는다. 파일명을 고정하면 pom.xml의 version을 올릴 때마다 이 줄이 깨진다.
COPY --from=build /app/target/*.jar app.jar
# 최소 권한: 비루트 실행 — runAsNonRoot 정책 충족(root면 기동 실패).
# /usr/bin/pebble 은 베이스 이미지가 담고 있는 9.5MB 바이너리다. 이 이미지 안에서 그것을
#   참조하는 곳이 없고(베이스 엔트리포인트·/etc·/opt·/usr/local 모두 0건), 우리 ENTRYPOINT 는
#   java 를 직접 띄운다 — 실행될 경로가 없다.
#   지우지 않으면 그 바이너리에 컴파일된 Go 표준 라이브러리의 취약점을 이미지 스캔이 잡아
#   파이프라인이 멈춘다. 업스트림이 고쳐진 Go 로 다시 빌드하면 이 줄은 없어도 된다.
RUN rm -f /usr/bin/pebble \
 && useradd -r -u 1001 spring
USER 1001
# 컨테이너 메모리 한도에 힙 맞춤(cgroup 인식). PORT는 compose/k8s가 주입.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
EXPOSE 8091
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
