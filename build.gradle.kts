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

    implementation("com.fasterxml.jackson.core:jackson-core:${BuildVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${BuildVersions.jackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:${BuildVersions.jackson}")

    // compression
    implementation("org.xerial.snappy:snappy-java:1.1.9.1")
    implementation("org.anarres.lzo:lzo-core:1.0.6")

    implementation("org.pcollections:pcollections:4.0.1")

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

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass.set("MainKt")
}