plugins {
    kotlin("jvm")
    alias(libs.plugins.dokka)
    id("maven-publish")
    id("signing")
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
    implementation(libs.snappy)

    testImplementation(libs.bundles.testing)
}

tasks.test {
    useJUnitPlatform()
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
    publications {
        create<MavenPublication>("lsm4k-snappy-compressor") {
            from(components["java"])

            artifact(sourcesJar)
            artifact(dokkaJavadocJar)

            pom {
                name.set("LSM4K Snappy Compressor")
                description.set("A Snappy-based Compressor implementation for LSM4K.")
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
    sign(publishing.publications["lsm4k-snappy-compressor"])
}