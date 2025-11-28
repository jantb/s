package app

import LogLevel
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.ExperimentalTime

@Serializable
data class EcsDocument2 @OptIn(ExperimentalTime::class) constructor(
    @SerialName("@timestamp") val timestamp: kotlin.time.Instant,
    val log: LogInfo,
    val process: ProcessInfo,
    val service: ServiceInfo,
    val message: String,
    val correlation: CorrelationInfo? = null,
    val request: RequestInfo? = null,
    val ecs: EcsInfo? = null,
    val error: ErrorInfo? = null,
)

@Serializable
data class LogInfo(
    val level: LogLevel,
    val logger: String
)

@Serializable
data class ErrorInfo(
    val message: String?=null,
    @SerialName("stack_trace") val stacktrace: String?=null
)

@Serializable
data class ProcessInfo(
    val pid: Int,
    val thread: ThreadInfo
)

@Serializable
data class ThreadInfo(
    val name: String
)

@Serializable
data class ServiceInfo(
    val name: String,
    val version: String,
    val node: Map<String, String> = emptyMap()
)

@Serializable
data class EcsInfo(
    val version: String
)
@Serializable
data class CorrelationInfo(
    val id: String
)

@Serializable
data class RequestInfo(
    val id: String
)

