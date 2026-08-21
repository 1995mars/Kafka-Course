FROM eclipse-temurin:21-jre
WORKDIR /app
# jar phải được build TRƯỚC bằng: mvn -f consumer/pom.xml clean package
COPY target/scaling-consumer.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
