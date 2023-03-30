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

    testImplementation(platform("org.junit:junit-bom:${BuildVersions.jUnit5}"))
    testImplementation("org.junit.jupiter", "junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("io.strikt","strikt-core", BuildVersions.strikt)
    testImplementation("ch.qos.logback", "logback-classic", BuildVersions.logback)
    testImplementation(kotlin("reflect"))
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