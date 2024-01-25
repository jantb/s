plugins {
    kotlin("jvm") version "1.9.21"
    id("com.github.johnrengelman.shadow") version "7.1.2"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("net.openhft:zero-allocation-hashing:0.16")
    implementation("io.fabric8:kubernetes-client:6.9.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.16.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "2048m"
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("MainKt")
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    manifest {
        attributes["Main-Class"] = "MainKt"
    }
}
