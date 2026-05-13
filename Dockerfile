FROM openjdk:21-slim
WORKDIR /app
COPY target/motion-event-analytics-kstreams.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
