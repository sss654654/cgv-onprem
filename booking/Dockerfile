# booking(Java/Spring) 멀티스테이지 — Maven 빌드 → JRE 런타임.
# 풋프린트 주의(§3-3): JVM은 무겁다 → 컨테이너 인식 옵션으로 힙 제한. 2-2에서 GraalVM/AppCDS 검토.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
# 빌드 JVM 힙 상한. 아래 runtime 스테이지의 JAVA_OPTS는 그 스테이지 안에서만 유효해
#   빌드하는 JVM은 그 값을 못 본다. 안 주면 JVM이 cgroup이 보고하는 메모리의 25%를
#   힙으로 잡는데, 러너가 도는 VM 상한(6GB) 기준이라 1.5GB까지 커진다.
ENV MAVEN_OPTS="-Xmx768m"
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
# 와일드카드로 받는다. 파일명을 고정하면 pom.xml의 version을 올릴 때마다 이 줄이 깨진다.
COPY --from=build /app/target/*.jar app.jar
# 최소 권한(§5-A): 비루트 실행 — runAsNonRoot 정책 충족(root면 기동 실패).
RUN useradd -r -u 1001 spring
USER 1001
# 컨테이너 메모리 한도에 힙 맞춤(cgroup 인식). PORT는 compose/k8s가 주입.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
EXPOSE 8091
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
