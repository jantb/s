package app

import LogLevel
import kotlinx.serialization.Serializable

@Serializable
data class KafkaLineDomain(
    override val seq: Long,
    override val level: LogLevel,
    override val timestamp: Long,
    override val message: String,
    override val indexIdentifier: String,
    val topic: String,
    val key: String?,
    val offset: Long,
    val partition: Int,
    val headers: String,
    val correlationId: String? = null,
    val requestId: String? = null,
    val compositeEventId: String,
) : DomainLine {

    override fun toString(): String = buildString {
        append(seq).append(" ")
        append(level).append(" ")
        append(timestamp).append(" ")
        append(message).append(" ")
        append(indexIdentifier).append(" ")
        append(topic).append(" ")
        append(key).append(" ")
        append(offset).append(" ")
        append(partition).append(" ")
        append(headers).append(" ")
        append(correlationId).append(" ")
        append(requestId).append(" ")
        append(compositeEventId)
    }.lowercase()
    override fun compareTo(other: DomainLine): Int {
        return timestamp.compareTo(other.timestamp)
    }
    override fun contains(
        queryList: List<String>,
        queryListNot: List<String>,
    ): Boolean {
        val cachedString = toString()
        return when {
            queryList.isEmpty() && queryListNot.isEmpty() -> true
            queryList.isEmpty() -> queryListNot.none { cachedString.contains(it, ignoreCase = true) }
            queryListNot.isNotEmpty() -> queryList.all { cachedString.contains(it, ignoreCase = true) } &&
                    queryListNot.none { cachedString.contains(it, ignoreCase = true) }

            else -> queryList.all { cachedString.contains(it, ignoreCase = true) }
        }
    }
}