FROM eclipse-temurin:21-jre
WORKDIR /app
# jar phải được build TRƯỚC bằng: mvn -f producer/pom.xml clean package
COPY target/scaling-producer.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
