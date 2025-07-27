package web

import app.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.http.content.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.serialization.Serializable
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.seconds
import kafka.Kafka

class WebServer(private val port: Int = 9999) {
    private val clients = ConcurrentHashMap<String, WebSocketSession>()
    private val clientPods = ConcurrentHashMap<String, MutableSet<String>>()
    private val json = kotlinx.serialization.json.Json {
        ignoreUnknownKeys = true
        prettyPrint = false
        isLenient = true
        encodeDefaults = true
    }

    fun start() {
        embeddedServer(Netty, port = port) {
            install(WebSockets) {
                pingPeriod = 30.seconds // Increased to avoid aggressive pings
                timeout = 60.seconds // Increased to give client more time
                maxFrameSize = Long.MAX_VALUE
                masking = false
            }

            install(ContentNegotiation) {
                json()
            }

            routing {
                staticResources("/", "static", "index.html")

                // WebSocket endpoint for log streaming
                webSocket("/logs") {
                    val clientId = System.currentTimeMillis().toString()
                    clients[clientId] = this
                    clientPods[clientId] = mutableSetOf()
                    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

                    try {
                        // Send a welcome message to verify connection
                        send(Frame.Text(json.encodeToString(
                            WebSocketMessage.serializer(),
                            WebSocketMessage("welcome", logs = listOf(WebLogLine(
                                timestamp = System.currentTimeMillis(),
                                level = "INFO",
                                message = "WebSocket connection established",
                                logger = "WebServer",
                                serviceName = "system"
                            )))
                        )))

                        // Start a job to periodically send stats to the client
                        val statsJob = scope.launch {
                            try {
                                while (isActive) {
                                    try {
                                        withTimeout(500) {
                                            try {
                                                send(Frame.Text(json.encodeToString(
                                                    StatsMessage.serializer(),
                                                    StatsMessage(State.indexedLines.get())
                                                )))
                                            } catch (e: Exception) {

                                            }
                                        }
                                    } catch (e: Exception) {
                                        // Ignore timeout exceptions
                                    }
                                    delay(1000)
                                }
                            } catch (e: CancellationException) {

                            } catch (e: Exception) {

                            }
                        }

                        // Start the coroutine to listen for log updates
                        val logUpdateJob = scope.launch {
                            try {
                                while (isActive) {
                                    try {
                                        val message = Channels.kafkaCmdGuiChannel.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
                                        if (message is ResultChanged) {
                                            try {
                                                val logs = message.result.map { line ->
                                                    when (line) {
                                                        is LogLineDomain -> WebLogLine(
                                                            timestamp = line.timestamp,
                                                            level = line.level.toString(),
                                                            message = line.serviceName + " " + line.message.take(5000),
                                                            logger = line.logger,
                                                            serviceName = line.serviceName
                                                        )
                                                        is KafkaLineDomain -> WebLogLine(
                                                            timestamp = line.timestamp,
                                                            level = "KAFKA",
                                                            message = line.toString().take(5000),
                                                            logger = "",
                                                            serviceName = line.topic
                                                        )
                                                    }
                                                }.take(500)

                                                if (logs.isNotEmpty()) {
                                                    withTimeout(1000) {
                                                        try {
                                                            send(Frame.Text(json.encodeToString(
                                                                WebSocketMessage.serializer(),
                                                                WebSocketMessage("logs", logs = logs)
                                                            )))
                                                        } catch (e: Exception) {
                                                            println("Failed to send logs for client $clientId: ${e.message}")
                                                        }
                                                    }
                                                }
                                            } catch (e: Exception) {
                                                println("Error processing log message for client $clientId: ${e.message}")
                                            }
                                        }
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Exception) {
                                        println("Error processing logs for client $clientId: ${e.message}")
                                    }
                                    yield()
                                }
                            } catch (e: CancellationException) {
                                println("Log update job cancelled for client $clientId: ${e.message}")
                            } catch (e: Exception) {
                                println("Log update job error for client $clientId: ${e.message}")
                            }
                        }

                        // Start the coroutine to listen for log cluster updates
                        val logClusterJob = scope.launch {
                            try {
                                while (isActive) {
                                    try {
                                        val message = Channels.logClusterCmdGuiChannel.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
                                        if (message is LogClusterList) {
                                            try {
                                                val clusters = message.clusters.map { cluster ->
                                                    LogClusterInfo(
                                                        count = cluster.count.toInt(),
                                                        level = cluster.level.name,
                                                        indexIdentifier = cluster.indexIdentifier,
                                                        messagePattern = cluster.block
                                                    )
                                                }

                                                withTimeout(1000) {
                                                    try {
                                                        send(Frame.Text(json.encodeToString(
                                                            WebSocketMessage.serializer(),
                                                            WebSocketMessage("logClusters", logClusters = clusters)
                                                        )))
                                                    } catch (e: Exception) {
                                                        println("Failed to send log clusters for client $clientId: ${e.message}")
                                                    }
                                                }
                                            } catch (e: Exception) {
                                                println("Error processing log cluster message for client $clientId: ${e.message}")
                                            }
                                        }
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Exception) {
                                        println("Error processing log clusters for client $clientId: ${e.message}")
                                    }
                                    yield()
                                }
                            } catch (e: CancellationException) {
                                println("Log cluster job cancelled for client $clientId: ${e.message}")
                            } catch (e: Exception) {
                                println("Log cluster job error for client $clientId: ${e.message}")
                            }
                        }

                        // Start the coroutine to listen for chart data updates
                        val chartDataJob = scope.launch {
                            try {
                                while (isActive) {
                                    try {
                                        val message = Channels.kafkaCmdGuiChannel.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
                                        if (message is ResultChanged) {
                                            try {
                                                // Send chart data if available
                                                if (message.chartResult.isNotEmpty()) {
                                                    // Create chart data from the result
                                                    val chartResult = message.chartResult
                                                    
                                                    // For simplicity, we'll send a simplified version of the data
                                                    // In a real implementation, this would be more sophisticated
                                                    val timePoints = chartResult.take(100).map { domainLine ->
                                                        // Create a simple time point representation
                                                        val time = domainLine.timestamp
                                                        // For now, we'll just send a simple count per level
                                                        // A real implementation would aggregate data properly
                                                        val counts = listOf(1, 0, 0, 0, 0, 0) // Simplified counts for INFO, WARN, DEBUG, ERROR, UNKNOWN, KAFKA
                                                        TimePointData(time, counts)
                                                    }
                                                    
                                                    val levels = listOf("INFO", "WARN", "DEBUG", "ERROR", "UNKNOWN", "KAFKA")
                                                    val chartData = LogLevelChartData(timePoints, levels, 100)
                                                    
                                                    withTimeout(1000) {
                                                        try {
                                                            send(Frame.Text(json.encodeToString(
                                                                WebSocketMessage.serializer(),
                                                                WebSocketMessage("chartData", chartData = chartData)
                                                            )))
                                                        } catch (e: Exception) {
                                                            println("Failed to send chart data for client $clientId: ${e.message}")
                                                        }
                                                    }
                                                }
                                            } catch (e: Exception) {
                                                println("Error processing chart data message for client $clientId: ${e.message}")
                                            }
                                        }
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Exception) {
                                        println("Error processing chart data for client $clientId: ${e.message}")
                                    }
                                    yield()
                                }
                            } catch (e: CancellationException) {
                                println("Chart data job cancelled for client $clientId: ${e.message}")
                            } catch (e: Exception) {
                                println("Chart data job error for client $clientId: ${e.message}")
                            }
                        }

                        // Set up message handler for incoming client messages
                        val messageHandlerJob = scope.launch {
                            try {
                                for (frame in incoming) {
                                    when (frame) {
                                        is Frame.Text -> {
                                            try {
                                                val message = json.decodeFromString<ClientMessage>(frame.readText())
                                                when (message.action) {
                                                    "listPods" -> {
                                                        val listPods = ListPods()
                                                        Channels.podsChannel.put(listPods)
                                                        val pods = listPods.result.get()
                                                        val podMaps = pods.map { pod ->
                                                            mapOf(
                                                                "name" to pod.name,
                                                                "version" to pod.version,
                                                                "creationTimestamp" to pod.creationTimestamp
                                                            )
                                                        }
                                                        send(Frame.Text(json.encodeToString(
                                                            WebSocketMessage.serializer(),
                                                            WebSocketMessage("pods", podMaps = podMaps)
                                                        )))
                                                    }
                                                    "listenPod" -> {
                                                        message.podName?.let {
                                                            Channels.podsChannel.put(ListenToPod(it))
                                                            clientPods[clientId]?.add(it)
                                                        }
                                                    }
                                                    "unlistenPod" -> {
                                                        message.podName?.let {
                                                            Channels.podsChannel.put(UnListenToPod(it))
                                                            clientPods[clientId]?.remove(it)
                                                        }
                                                    }
                                                    "search" -> {
                                                        message.query?.let {
                                                            try {
                                                                Channels.searchChannel.trySendBlocking(
                                                                    QueryChanged(
                                                                        it,
                                                                        length = message.length,
                                                                        offset = message.offset
                                                                    )
                                                                )
                                                            } catch (e: Exception) {
                                                                println("Error sending search query for client $clientId: ${e.message}")
                                                            }
                                                        }
                                                    }
                                                    "ping" -> {
                                                    }
                                                    "listTopics" -> {
                                                        val listTopics = ListTopics()
                                                        Channels.kafkaChannel.put(listTopics)
                                                        try {
                                                            val topics = listTopics.result.get()
                                                            val topicList = topics.map { KafkaTopic(it) }
                                                            send(Frame.Text(json.encodeToString(
                                                                WebSocketMessage.serializer(),
                                                                WebSocketMessage("topics", topics = topicList)
                                                            )))
                                                        } catch (e: Exception) {
                                                            println("Error listing Kafka topics for client $clientId: ${e.message}")
                                                        }
                                                    }
                                                    "listenToTopics" -> {
                                                        message.topics?.let {
                                                            Channels.kafkaChannel.put(ListenToTopic(it))
                                                        }
                                                    }
                                                    "unlistenToTopics" -> {
                                                        Channels.kafkaChannel.put(UnListenToTopics)
                                                    }
                                                    "listLag" -> {
                                                        try {
                                                            val msg = ListLag()
                                                            Channels.kafkaChannel.put(msg)
                                                            val result = msg.result.await()
                                                            val lagInfoList = result.map { lagInfo ->
                                                                KafkaLagInfo(
                                                                    groupId = lagInfo.groupId,
                                                                    topic = lagInfo.topic,
                                                                    partition = lagInfo.partition,
                                                                    currentOffset = lagInfo.currentOffset,
                                                                    endOffset = lagInfo.endOffset,
                                                                    lag = lagInfo.lag
                                                                )
                                                            }
                                                            send(Frame.Text(json.encodeToString(
                                                                WebSocketMessage.serializer(),
                                                                WebSocketMessage("lagInfo", lagInfo = lagInfoList)
                                                            )))
                                                        } catch (e: Exception) {
                                                            println("Error listing Kafka lag for client $clientId: ${e.message}")
                                                        }
                                                    }
                                                    "refreshLogGroups" -> {
                                                        try {
                                                            Channels.refreshChannel.trySend(RefreshLogGroups)
                                                        } catch (e: Exception) {
                                                            println("Error refreshing log groups for client $clientId: ${e.message}")
                                                        }
                                                    }
                                                    "setKafkaDays" -> {
                                                        message.days?.let {
                                                            State.kafkaDays.set(it.toLong())
                                                        }
                                                    }
                                                }
                                            } catch (e: Exception) {
                                                println("Error parsing client message for client $clientId: ${e.message}")
                                            }
                                        }
                                        is Frame.Close -> {
                                            break // Exit the loop to trigger cleanup
                                        }
                                        else -> {
                                        }
                                    }
                                }
                            } catch (e: CancellationException) {
                            } catch (e: Exception) {
                            }
                        }

                        // Wait for the session to complete
                        messageHandlerJob.join()
                        logUpdateJob.join()
                        logClusterJob.join()
                        chartDataJob.join()

                    } catch (e: Exception) {
                        println("Error in WebSocket session for client $clientId: ${e.message}")
                    } finally {
                        // Clean up client-specific resources
                        clients.remove(clientId)
                        try {
                            // Cancel all coroutines in the scope
                            scope.cancel("WebSocket session closed for client $clientId")

                            // Stop listening to any pods this client was monitoring
                            clientPods[clientId]?.forEach { podName ->
                                Channels.podsChannel.put(UnListenToPod(podName))
                            }
                            clientPods.remove(clientId)
                        } catch (e: Exception) {
                            println("Error during cleanup for client $clientId: ${e.message}")
                        }
                    }
                }
            }
        }.start(wait = true)
    }
}

@Serializable
data class WebLogLine(
    val timestamp: Long,
    val level: String,
    val message: String,
    val logger: String,
    val serviceName: String
)

@Serializable
data class WebSocketMessage(
    val type: String,
    val logs: List<WebLogLine> = emptyList(),
    val podMaps: List<Map<String, String>> = emptyList(),
    val topics: List<KafkaTopic> = emptyList(),
    val lagInfo: List<KafkaLagInfo> = emptyList(),
    val logClusters: List<LogClusterInfo> = emptyList(),
    val chartData: LogLevelChartData? = null
)

@Serializable
data class StatsMessage(
    val indexedLines: Int,
    val type: String = "stats"
)

@Serializable
data class ClientMessage(
    val action: String,
    val podName: String? = null,
    val query: String? = null,
    val offset: Int = 0,
    val length: Int = 0,
    val levels: List<String>? = null,
    val topics: List<String>? = null,
    val hideLowSeverity: Boolean = false,
    val sortByCount: Boolean = false,
    val hideTopicsWithoutLag: Boolean = false,
    val sortByLag: Boolean = false,
    val startTime: Long? = null,
    val endTime: Long? = null,
    val minLogLevel: String? = null,
    val serviceNames: List<String>? = null,
    val correlationIds: List<String>? = null,
    val days: Int? = null
)

@Serializable
data class KafkaTopic(
    val name: String
)

@Serializable
data class KafkaLagInfo(
    val groupId: String,
    val topic: String,
    val partition: Int,
    val currentOffset: Long,
    val endOffset: Long,
    val lag: Long
)

@Serializable
data class LogClusterInfo(
    val count: Int,
    val level: String,
    val indexIdentifier: String,
    val messagePattern: String
)

@Serializable
data class LogLevelChartData(
    val timePoints: List<TimePointData>,
    val levels: List<String>,
    val scaleMax: Int
)

@Serializable
data class TimePointData(
    val time: Long,
    val counts: List<Int>
)