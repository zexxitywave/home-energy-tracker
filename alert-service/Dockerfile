FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY target/alert-service-0.0.1-SNAPSHOT-exec.jar app.jar

EXPOSE 8084

ENTRYPOINT ["java","-jar","app.jar"]