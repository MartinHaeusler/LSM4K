plugins {
    kotlin("jvm") version "1.8.0"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.slf4j:slf4j-api:2.0.5")
    implementation("io.github.microutils:kotlin-logging:2.1.23")
    implementation("com.google.guava:guava:31.1-jre")
    testImplementation(kotlin("test"))
    testImplementation("ch.qos.logback:logback-classic:1.4.6")
    testImplementation("io.strikt:strikt-core:0.34.1")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("MainKt")
}