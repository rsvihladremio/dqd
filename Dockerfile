FROM eclipse-temurin:17

RUN mkdir /app
WORKDIR /app
COPY . /app

RUN ls /app
RUN ./mvnw package -DskipTests=true
RUN find . | grep jar-with- | xargs -I {} mv {} dqd.jar
RUN rm -fr target

FROM --platform=linux/amd64 amd64/eclipse-temurin:17
WORKDIR /root/
COPY --from=0 /app/dqd.jar /root/dqd.jar
CMD ["java", "-jar", "dqd.jar", "server", "-v"]

EXPOSE 8080
