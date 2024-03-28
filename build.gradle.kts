plugins {
    kotlin("jvm") version "1.9.23"
}

group = "com.igorgoose"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework:spring-websocket:6.1.5")
    implementation("org.springframework:spring-messaging:6.1.5")
    implementation("org.glassfish.tyrus.bundles:tyrus-standalone-client:2.1.5")
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("ch.qos.logback:logback-classic:1.5.3")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}