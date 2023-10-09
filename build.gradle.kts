plugins {
    kotlin("jvm") version libs.versions.kotlin
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.bundles.loggingApi)

    implementation(libs.guava)

    implementation(libs.bundles.jackson)

    // compression
    implementation(libs.snappy)
    implementation(libs.lz4J)
    implementation(libs.zstdJni)

    implementation(libs.pcollections)

    testImplementation(libs.bundles.jUnit5)
    testImplementation(libs.logback)
    testImplementation(libs.strikt)

    // for benchmarking purposes, include some other key-value stores
    testImplementation(libs.bundles.xodus)

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