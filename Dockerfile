FROM eclipse-temurin:21-jdk-alpine as build

ENV SRC_HOME=/home/build
WORKDIR $SRC_HOME

COPY gradlew settings.gradle.kts gradle.properties $SRC_HOME/
COPY gradle $SRC_HOME/gradle
COPY build.gradle.kts $SRC_HOME/
RUN chmod +x gradlew && ./gradlew build || return 0

COPY . .
RUN ./gradlew bootJar


FROM eclipse-temurin:21-jre-alpine

ENV SRC_HOME=/home/build

COPY --from=build $SRC_HOME/build/libs/racehelpr-scraper.jar /opt/racehelpr/racehelpr-scraper.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/opt/racehelpr/speller-server.jar"]
