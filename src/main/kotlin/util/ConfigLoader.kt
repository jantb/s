package util

import java.io.File
import kotlin.system.exitProcess

class ConfigLoader {
    private val config: Map<String, String>

    init {
        val homeDir = System.getProperty("user.home")
        val configFile = File(homeDir, ".search.conf")

        if (!configFile.exists()) {
            System.err.println("Config file not found: ${configFile.absolutePath}")
            exitProcess(1)
        }

        config = configFile.readLines()
            .filter { it.contains("=") }
            .map { line ->
                val (key, value) = line.split("=", limit = 2)
                key.trim() to value.trim()
            }
            .toMap()
    }

    fun getValue(key: String): String {
        return config[key] ?: throw IllegalArgumentException("Config key not found: $key")
    }

    // Optional: Get with default value
    fun getValueOrDefault(key: String, default: String): String {
        return config[key] ?: default
    }
}
