FROM openjdk:8

LABEL Microservice="Timbermill2-Server"
ENV XMX=8g
ENV XMS=3g

# Prepare environment:
RUN mkdir -p /opt/microservice

# Get the app
COPY ./timbermill-java/timbermill-server/target/timbermill-server-*.jar /opt/microservice/microservice.jar
COPY ./timbermill-java/timbermill-server/target/test-classes/log4j2.xml /opt/microservice/resources/log4j.xml

# Expose the app port
EXPOSE 8484

WORKDIR /opt/microservice

# Run the app
CMD java \
    -Xmx$XMX -Xms$XMS \
    -jar /opt/microservice/microservice.jar
