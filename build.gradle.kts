import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import org.jreleaser.model.Active
import org.jreleaser.model.Signing

plugins {
    kotlin("jvm") version libs.versions.kotlin
    alias(libs.plugins.versions)
    alias(libs.plugins.dokka)
    alias(libs.plugins.jreleaser)
    id("maven-publish")
    id("signing")
}

group = "io.github.martinhaeusler.lsm4k"
version = "1.0.0-M1"
description = "LSM4K - A Transactional Key-Value Store in Kotlin"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.bundles.loggingApi)
    implementation(libs.bundles.jackson)

    // COMPRESSION SPI
    implementation(project(":compressor-api"))

    // UTILS
    implementation(libs.guava)
    implementation(libs.pcollections)
    implementation(libs.cronUtils)

    // TESTING
    testImplementation(libs.bundles.testing)
    testImplementation(libs.logback)
    testImplementation(project(":compressor-snappy"))
    testImplementation(project(":compressor-lz4"))
    testImplementation(project(":compressor-zstd"))


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

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
}

val dokkaJavadocJar = tasks.register<Jar>("dokkaJavadocJar") {
    dependsOn(tasks.dokkaJavadoc)
    from(tasks.dokkaJavadoc.flatMap { it.outputDirectory })
    archiveClassifier.set("javadoc")
}

artifacts {
    add("archives", sourcesJar)
    add("archives", dokkaJavadocJar)
}

publishing {
    repositories {
        maven {
            name = "LocalMavenWithChecksums"
            url = uri(layout.buildDirectory.dir("staging-deploy"))
        }

        maven {
            name = "PreDeploy"
            url = uri(layout.buildDirectory.dir("pre-deploy"))
        }
    }

    publications {
        create<MavenPublication>("lsm4k") {
            from(components["java"])

            artifact(sourcesJar)
            artifact(dokkaJavadocJar)

            pom {
                name.set("LSM4K")
                description.set("A Transactional Key-Value store based on Log-Structured-Merge-Trees in Kotlin")
                url.set("https://github.com/MartinHaeusler/LSM4K")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("martin.haeusler")
                        name.set("Martin Häusler")
                        email.set("martin.haeusler89@gmail.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/MartinHaeusler/LSM4K.git")
                    developerConnection.set("scm:git:ssh://github.com:MartinHaeusler/LSM4K.git")
                    url.set("https://github.com/MartinHaeusler/LSM4K")
                }
            }
        }
    }
}

signing {
    setRequired {
        gradle.taskGraph.allTasks.any { it.name.contains("LocalMavenWithChecksums") }
    }
    sign(publishing.publications["lsm4k"])
}


jreleaser {
    signing {
        active = Active.ALWAYS
        armored = true
        mode = Signing.Mode.FILE
        publicKey = "/home/martin/.gnupg/public.key"
        secretKey = "/home/martin/.gnupg/private.key"
    }
    deploy {
        maven {
            mavenCentral {
                create("sonatype") {
                    active = Active.ALWAYS
                    url = "https://central.sonatype.com/api/v1/publisher"
                    stagingRepository(layout.buildDirectory.dir("pre-deploy").get().asFile.path)
                }
            }
        }
    }
    release {
        github {
            enabled = false
        }
    }
}

allprojects {

    tasks.withType<Jar>().configureEach {
        doLast {
            ant.withGroovyBuilder {
                "checksum"("algorithm" to "md5", "file" to archiveFile.get().asFile)
                "checksum"("algorithm" to "sha1", "file" to archiveFile.get().asFile)
            }
        }
    }

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
