FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd -r -u 10001 jobsvc
COPY --from=build /app/target/async-job-service-1.0.0.jar app.jar
USER jobsvc
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
