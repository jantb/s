import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    implementation("io.fabric8:kubernetes-client:7.4.0")
    implementation("com.squareup.okhttp3:okhttp:5.0.0-alpha.14")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.apache.kafka:kafka-clients:4.1.1")
    implementation("io.confluent:kafka-avro-serializer:8.0.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
    runtimeOnly("org.jetbrains.kotlinx:kotlinx-serialization-core-jvm:1.9.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "2048m"
}

tasks.register<Jar>("fatJar") {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Main-Class"] = "MainKt"
    }
    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    })
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("MainKt")
    applicationDefaultJvmArgs = listOf("--add-exports=java.desktop/com.apple.eawt=ALL-UNNAMED")
}


val compileKotlin: KotlinCompile by tasks
compileKotlin.compilerOptions {
    freeCompilerArgs.set(listOf("-Xannotation-default-target=param-property"))
}