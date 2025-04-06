import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask

plugins {
    kotlin("jvm") version libs.versions.kotlin
    alias(libs.plugins.versions)
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

    implementation(libs.cronUtils)

    // TESTING
    testImplementation(libs.bundles.jUnit5)
    testImplementation(libs.logback)
    testImplementation(libs.strikt)
    testImplementation(libs.awaitility)

    // for benchmarking purposes, include some other key-value stores
    testImplementation(libs.bundles.xodus)
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(libs.versions.java.get().toInt())
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(libs.versions.java.get()))
    }
}

application {
    mainClass.set("MainKt")
}


// ================================================================================================
// VERSION UPDATES
// https://github.com/ben-manes/gradle-versions-plugin
// ================================================================================================

tasks.withType<DependencyUpdatesTask> {
    rejectVersionIf {
        isNonStable(candidate.version) && !isNonStable(currentVersion)
    }
}

private fun isNonStable(version: String): Boolean {
    val stableKeyword = listOf("RELEASE", "FINAL", "GA").any { version.uppercase().contains(it) }
    val regex = "^[0-9,.v-]+(-r)?$".toRegex()
    val isStable = stableKeyword || regex.matches(version)
    return isStable.not()
}
