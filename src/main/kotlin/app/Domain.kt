package app

import LogLevel
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import kafka.RawJsonDeserializer
import kafka.RawJsonSerializer
import java.time.Instant
data class Domain(
    var seq :Long,
    var indexIdentifier: String = "",
    @JsonProperty("@timestamp") val timestampString: String = "",
    @JsonAlias("correlation.id", "X-Correlation-Id") val correlationId: String = "",
    @JsonAlias("request.id", "X-Request-Id") val requestId: String = "",
    @JsonAlias("message", "msg") var message: String = "",
    @JsonAlias("error.message") val errorMessage: String? = "",
    @JsonAlias("log.level", "level") var level: LogLevel = LogLevel.UNKNOWN,
    @JsonAlias("application", "service.name") val application: String = "",
    @JsonAlias("error.type") val stacktraceType: String = "",
    @JsonAlias("stack_trace", "error.stack_trace") val stacktrace: String = "",
    @JsonProperty("topic") val topic: String = "",
    @JsonProperty("key") val key: String = "",
    @JsonProperty("partition") val partition: String = "",
    @JsonProperty("offset") val offset: String = "",
    @JsonProperty("headers") val headers: String = "",
    @JsonDeserialize(using = RawJsonDeserializer::class) @JsonSerialize(using = RawJsonSerializer::class) @JsonProperty(
        "z"
    )
    val data: String = "",
    ) :  Comparable<Domain>  {
    var timestamp: Long = 0
    private var searchableString = ""

    fun parseTimestamp(): Domain {
        if (timestamp == 0L && timestampString.isNotBlank()) {
            timestamp = try {
                Instant.parse(timestampString).toEpochMilli()
            } catch (e: Exception) {
                0L
            }
        }
        init()
        return this
    }

    override fun compareTo(other: Domain): Int {
        return this.timestamp.compareTo(other.timestamp)
    }

    override fun toString(): String {
        return searchableString
    }

    fun init() {
        searchableString = listOf(
            timestamp,
            indexIdentifier,
            application,
            level,
            correlationId,
            requestId,
            message,
            errorMessage,
            stacktrace,
            stacktraceType,
            topic,
            key,
            partition,
            offset,
            headers,
            data
        ).joinToString(" ")
    }
}
