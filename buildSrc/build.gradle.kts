import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    // the "buildSrc" project is special in gradle and it doesn't have access
    // to the plugin version definitions from our root project. Therefore, we need
    // to specify the kotlin plugin version here explicitly.
    kotlin("jvm") version "1.8.0"
}

tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions {
        this.jvmTarget = "17"
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories {
    mavenCentral()
}