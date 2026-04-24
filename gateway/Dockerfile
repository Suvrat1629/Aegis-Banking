## Build stage
FROM gradle:8.14-jdk21 AS builder

WORKDIR /home/gradle

# Copy gateway-specific wrapper and build files first for layer caching
COPY gateway/build.gradle gateway/settings.gradle gateway/gradlew gateway/gradle/ /home/gradle/project/

# Copy contracts directory so Gradle can access shared proto/xsd files
COPY contracts/ /home/gradle/contracts/

WORKDIR /home/gradle/project
RUN chmod +x gradlew || true

# Download dependencies (cached layer if build files don't change) using the wrapper
RUN ./gradlew dependencies --no-daemon || true

# Copy the entire gateway source into the project dir
COPY gateway/ /home/gradle/project/

# Build the JAR using the Gradle wrapper
RUN ./gradlew clean bootJar --no-daemon

## Run stage
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy the JAR from builder
COPY --from=builder /home/gradle/project/build/libs/*.jar /app/app.jar

# Expose application and metrics ports
EXPOSE 8080 2112

ENTRYPOINT ["java", "-jar", "/app/app.jar"]