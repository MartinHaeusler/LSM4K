plugins {
    kotlin("jvm")
}

group = rootProject.group
version = rootProject.version

repositories {
    mavenCentral()
}


kotlin {
    jvmToolchain(libs.versions.java.get().toInt())
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(libs.versions.java.get()))
    }
    withJavadocJar()
    withSourcesJar()
}

dependencies {
    implementation(libs.bundles.loggingApi)

    testImplementation(libs.bundles.testing)
}

tasks.test {
    useJUnitPlatform()
}