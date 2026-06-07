FROM eclipse-temurin:23-jdk-alpine AS builder
WORKDIR /app
COPY gradlew ./
COPY gradle/ gradle/
COPY build.gradle settings.gradle ./
RUN ./gradlew dependencies --no-daemon || true
COPY src/ src/
RUN ./gradlew build -x test --no-daemon

FROM eclipse-temurin:23-jdk-alpine
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
