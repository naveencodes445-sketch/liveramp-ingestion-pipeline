FROM amazoncorretto:21-alpine
WORKDIR /app
COPY target/ingestion-pipeline-0.0.1-SNAPSHOT.jar app.jar
COPY gcp-key.json gcp-key.json
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]