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
    @JsonProperty("message") val message: String = "",
    @JsonProperty("level") val level: String = "",
    @JsonProperty("application") val application: String = "",
    @JsonProperty("stack_trace") val stacktrace: String = "",
) : Domain() {
    var timestamp = OffsetDateTime.MIN

    @Transient
    private var cachedSearchableString: String? = null

    override fun searchableString(): String {
        if (cachedSearchableString == null) {
            cachedSearchableString = listOf(timestamp, application, level, message, stacktrace).joinToString(" ")
        }
        return cachedSearchableString!!
    }


    override fun timestamp(): Long {
        return try {
           timestamp.toInstant().toEpochMilli()
        } catch (e: Exception) {
            0
        }
    }
}
