package app

import LogLevel
import kotlinx.serialization.Serializable

@Serializable
data class LogLineDomain(
    override val seq: Long,
    override val level: LogLevel,
    override val timestamp: Long,
    override val message: String,
    override val indexIdentifier: String,
    val threadName: String,
    val serviceName: String,
    val serviceVersion: String,
    val logger: String,
    val correlationId: String? = null,
    val requestId: String? = null,
    val errorMessage: String? = null,
    val stacktrace: String? = null,
) : DomainLine {

    override fun compareTo(other: DomainLine): Int {
        return timestamp.compareTo(other.timestamp)
    }
    companion object {
        operator fun invoke(seq: Long, indexIdentifier: String, ecsDocument: EcsDocument) = LogLineDomain(
            seq = seq,
            timestamp = ecsDocument.timestamp.toEpochMilliseconds(),
            level = ecsDocument.logLevel,
            threadName = ecsDocument.threadName,
            serviceName = ecsDocument.serviceName,
            serviceVersion = ecsDocument.serviceVersion,
            logger = ecsDocument.logger,
            message = ecsDocument.message,
            correlationId = ecsDocument.correlationId,
            requestId = ecsDocument.requestId,
            errorMessage = ecsDocument.errorMessage,
            stacktrace = ecsDocument.stacktrace,
            indexIdentifier = indexIdentifier,
        )
    }
    private val cachedString by lazy {
        listOfNotNull(
            seq.toString(),
            level.name,
            timestamp.toString(),
            message,
            indexIdentifier,
            threadName,
            serviceName,
            serviceVersion,
            logger,
            correlationId,
            requestId,
            errorMessage,
            stacktrace
        ).joinToString(" ").lowercase()
    }

    override fun toString(): String = cachedString

    override fun contains(
        queryList: List<String>,
        queryListNot: List<String>,
    ): Boolean {
        if (queryList.isEmpty() && queryListNot.isEmpty()) return true
        val target = cachedString
        if (queryList.isEmpty()) {
            return queryListNot.none { target.contains(it.lowercase()) }
        }
        if (queryListNot.isEmpty()) {
            return queryList.all { target.contains(it.lowercase()) } &&
                    queryListNot.none { target.contains(it.lowercase()) }
        }
        return queryList.all { target.contains(it.lowercase()) }
    }
}
