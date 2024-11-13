package app

import LRUCache
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import kafka.RawJsonDeserializer
import kafka.RawJsonSerializer
import java.time.Instant
import java.util.UUID

sealed class Domain {
    open fun searchableString(): String {
        return ""
    }

    open fun timestamp(): Instant {
        return Instant.MIN
    }

    open fun contains(other: String): Boolean {
        return false
    }

    open fun getPunct(): String {
        return ""
    }
}

data class LogJson(
    val id: UUID = UUID.randomUUID(),
    var indexIdentifier: String = "",
    @JsonProperty("@timestamp") val timestampString: String = "",
    @JsonAlias("correlation.id", "loyalityhub-correlation-id") val correlationId: String = "",
    @JsonAlias("message", "msg") val message: String = "",
    @JsonAlias("error.message") val errorMessage: String = "",
    @JsonAlias("log.level", "level") val level: String = "",
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
    ) val data: String = "",


    ) : Domain() {
    var timestamp: Instant = Instant.MIN
    private var punkt = ""
    private var searchableString = ""

    init {
        try {
            this.timestamp = Instant.parse(timestampString)
        } catch (e: Exception) {
            this.timestamp = Instant.MIN
        }
        this.init()
    }

    override fun searchableString(): String {
        return searchableString
    }

    var cache = LRUCache<String, Boolean>(10_000)

    override fun timestamp(): Instant {
        return timestamp
    }

    override fun contains(other: String): Boolean {
        return cache.computeIfAbsent(key) { searchableString().contains(other) }
    }

    override fun getPunct(): String {
        return punkt
    }

    fun init() {
        searchableString = listOf(
            timestamp,
            indexIdentifier,
            application,
            level,
            correlationId,
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
        punkt = searchableString.replace("[a-zA-Z0-9\\s]".toRegex(), "")
    }
}
