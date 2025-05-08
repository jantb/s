package app

import LogLevel
import kotlinx.datetime.Instant
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class EcsDocument (
    @SerialName("@timestamp") val timestamp: Instant,
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

