package app

import LogLevel
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.ExperimentalTime

@Serializable
data class EcsDocument @OptIn(ExperimentalTime::class) constructor(
    @SerialName("@timestamp") val timestamp: kotlin.time.Instant,
    @SerialName("log.level") val logLevel: LogLevel,
    @SerialName("process.thread.name") val threadName: String,
    @SerialName("service.name") val serviceName: String,
    @SerialName("service.version") val serviceVersion: String = "",
    @SerialName("log.logger") val logger: String,
    val message: String,
    @SerialName("correlation.id") val correlationId: String? = null,
    @SerialName("request.id") val requestId: String? = null,
    @SerialName("error.message") val errorMessage: String? = null,
    @SerialName("error.stack_trace") val stacktrace: String? = null,
)

