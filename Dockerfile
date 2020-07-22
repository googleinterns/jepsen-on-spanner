# Use the official maven/Java 8 image to create a build artifact.
# https://hub.docker.com/_/maven
#FROM gradle:jdk11 as builder
#FROM gcr.io/google.com/cloudsdktool/cloud-sdk:alpine
#RUN gcloud node set project jepsen-on-spanner-with-gke
#RUN gcloud auth application-default login

FROM adoptopenjdk:11-jre-hotspot

# Copy local code to the container image.
COPY ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar /app/Jepsen-on-spanner.jar
COPY ./test-node.json /app/test-node.json
WORKDIR /app
#COPY build.gradle gradlew /app/
#COPY gradle /app/gradle

# Build a release artifact.
#RUN ./gradlew shadowJar

# Use AdoptOpenJDK for base image.
# It's important to use OpenJDK 8u191 or above that has container support enabled.
# https://hub.docker.com/r/adoptopenjdk/openjdk8
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
#FROM adoptopenjdk:11-jre-hotspot

# Copy the jar to the production image from the builder stage.
#COPY --from=builder /app/build/libs/*.jar /Jepsen-on-spanner.jar

# Run the web service on container startup.
CMD ["java","-jar","/Jepsen-on-spanner.jar","--instance","jepsen","--database","test","--component","WORKER","--pID","1"]