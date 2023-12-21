package app

import LRUCache
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import kafka.RawJsonDeserializer
import kafka.RawJsonSerializer
import java.time.Instant
import java.time.OffsetDateTime
import java.util.UUID

sealed class Domain {
    open fun searchableString(): String {
        return ""
    }

    open fun timestamp():Instant{
        return Instant.MIN
    }

    open fun contains(other :String): Boolean {
        return false
    }

    open fun getPunct(): String {
        return ""
    }
}

data class LogJson(
    val id :UUID = UUID.randomUUID(),
    @JsonProperty("message") val message: String = "",
    @JsonProperty("level") val level: String = "",
    @JsonProperty("application") val application: String = "",
    @JsonProperty("stack_trace") val stacktrace: String = "",
    @JsonProperty("topic") val topic: String = "",
    @JsonProperty("key") val key: String = "",
    @JsonProperty("partition") val partition: String = "",
    @JsonProperty("offset") val offset: String = "",
    @JsonDeserialize(using = RawJsonDeserializer::class) @JsonSerialize(using = RawJsonSerializer::class) @JsonProperty("z") val data: String = "",


) : Domain() {
    var timestamp: Instant = Instant.MIN
    private var punkt = ""
    private var searchableString = ""

    override fun searchableString(): String {
        return searchableString
    }

    var cache= LRUCache<String, Boolean>(10_000)

    override fun timestamp(): Instant {
        return timestamp
    }

    override fun contains(other: String): Boolean {
       return cache.computeIfAbsent(key){searchableString().contains(other)}
    }

    override fun getPunct(): String {
        return punkt
    }

    fun init() {
        searchableString = listOf(timestamp, application, level, message, stacktrace, topic, key, partition, offset, data).joinToString(" ")
        punkt = searchableString.replace("[a-zA-Z0-9\\s]".toRegex(), "")
    }
}
