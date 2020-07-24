# Use AdoptOpenJDK for base image.
FROM adoptopenjdk:11-jre-hotspot

# Copy local code to the container image.
COPY ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar /app/Jepsen-on-spanner.jar
COPY ./test-config.json /app/test-config.json
WORKDIR /app

# Run the web service on container startup.
CMD ["java","-jar","/Jepsen-on-spanner.jar","--instance","jepsen","--database","test","--component","WORKER","--pID","1"]