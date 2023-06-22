package app

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.OffsetDateTime

sealed class Domain {
    open fun searchableString(): String {
        return ""
    }

    open fun timestamp(): Long {
        return 0L
    }
}

data class LogJson(
    @JsonProperty("@timestamp") val timestamp: String,
    @JsonProperty("message") val message: String,
    @JsonProperty("level") val level: String,
    @JsonProperty("application") val application: String,
    @JsonProperty("stacktrace") val stacktrace: String,
) : Domain() {
    override fun searchableString(): String {
        return listOf(timestamp, application, level, message, stacktrace).joinToString(" ")
    }

    override fun timestamp(): Long {
        return try {
            OffsetDateTime.parse(timestamp).toInstant().toEpochMilli()
        } catch (e: Exception) {
            0
        }
    }
}

data class LogEntry(val timestamp: OffsetDateTime, val level: String, val message: String) : Domain() {
    override fun searchableString(): String {
        return message
    }

    override fun timestamp(): Long {
        return try {
            timestamp.toInstant().toEpochMilli()
        } catch (e: Exception) {
            0
        }
    }
}
