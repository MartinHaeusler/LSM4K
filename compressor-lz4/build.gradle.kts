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
}

dependencies {
    api(project(":compressor-api"))
    implementation(libs.lz4J)

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
