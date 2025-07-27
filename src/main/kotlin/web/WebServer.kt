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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.seconds
import kafka.Kafka

class WebServer(private val port: Int = 9999) {
    private val clients = ConcurrentHashMap<String, WebSocketSession>()
    private val clientPods = ConcurrentHashMap<String, MutableSet<String>>()
    private val clientLocks = ConcurrentHashMap<String, kotlinx.coroutines.sync.Mutex>()
    private val json = kotlinx.serialization.json.Json {
        ignoreUnknownKeys = true
        prettyPrint = false
        isLenient = true
        encodeDefaults = true
    }

    fun start() {
        embeddedServer(Netty, port = port) {
            install(WebSockets) {
                pingPeriod = 60.seconds // Less aggressive pinging
                timeout = 120.seconds // More generous timeout
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
                    clientLocks[clientId] = Mutex()
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

                        // Safe send function with mutex locking and proper error handling
                        suspend fun safeSendWithLock(message: String): Boolean {
                            val mutex = clientLocks[clientId] ?: return false
                            return try {
                                mutex.withLock {
                                    if (isActive && !incoming.isClosedForReceive) {
                                        withTimeout(2000) {
                                            send(Frame.Text(message))
                                            true
                                        }
                                    } else {
                                        false
                                    }
                                }
                            } catch (e: CancellationException) {
                                // Channel was cancelled, don't log as error
                                false
                            } catch (e: Exception) {
                                // Only log non-cancellation errors
                                if (e !is kotlinx.coroutines.channels.ClosedSendChannelException) {
                                    println("Failed to send message for client $clientId: ${e.message}")
                                }
                                false
                            }
                        }

                        // Start a job to periodically send stats to the client
                        val statsJob = scope.launch {
                            try {
                                while (isActive) {
                                    try {
                                        safeSendWithLock(json.encodeToString(
                                            StatsMessage.serializer(),
                                            StatsMessage(State.indexedLines.get())
                                        ))
                                        delay(1000)
                                    } catch (e: CancellationException) {
                                        break
                                    } catch (e: Exception) {
                                        // Ignore other exceptions
                                    }
                                }
                            } catch (e: CancellationException) {
                                // Expected during cleanup
                            }
                        }

                        // Unified data processing job - handles all channel communications through single coroutine
                        val dataProcessingJob = scope.launch {
                            try {
                                while (isActive) {
                                    try {
                                        // Process log updates and chart data from kafkaCmdGuiChannel
                                        val kafkaMessage = Channels.kafkaCmdGuiChannel.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS)
                                        if (kafkaMessage is ResultChanged) {
                                            try {
                                                // Process log data
                                                val logs = kafkaMessage.result.map { line ->
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
                                                    safeSendWithLock(json.encodeToString(
                                                        WebSocketMessage.serializer(),
                                                        WebSocketMessage("logs", logs = logs)
                                                    ))
                                                }

                                                // Process chart data from the same message
                                                val fullDataset = kafkaMessage.chartResult.ifEmpty { kafkaMessage.result }
                                                
                                                if (fullDataset.isNotEmpty()) {
                                                    // Dynamically determine time range based on actual data
                                                    val timestamps = fullDataset.map { it.timestamp }.sorted()
                                                    val minTime = timestamps.first()
                                                    val maxTime = timestamps.last()
                                                    val timeRange = maxTime - minTime
                                                    
                                                    // Calculate appropriate number of time windows (aim for ~100 data points)
                                                    val targetWindows = 100
                                                    val timeWindowMs = if (timeRange > 0) {
                                                        kotlin.math.max(60000L, timeRange / targetWindows) // At least 1 minute windows
                                                    } else {
                                                        60000L // Default to 1 minute if no time range
                                                    }
                                                    
                                                    val actualWindows = ((timeRange / timeWindowMs).toInt() + 1).coerceAtMost(200) // Max 200 windows
                                                    val timePoints = mutableListOf<TimePointData>()
                                                    
                                                    // Create time windows based on actual data range
                                                    for (i in 0 until actualWindows) {
                                                        val windowStart = minTime + (i * timeWindowMs)
                                                        val windowEnd = windowStart + timeWindowMs
                                                        
                                                        // Count logs by level in this time window from the FULL dataset
                                                        val levelCounts = mutableMapOf<String, Int>()
                                                        fullDataset.forEach { log ->
                                                            if (log.timestamp >= windowStart && log.timestamp < windowEnd) {
                                                                val levelName = log.level.name
                                                                levelCounts[levelName] = levelCounts.getOrDefault(levelName, 0) + 1
                                                            }
                                                        }
                                                        
                                                        // Convert to ordered counts array
                                                        val levels = listOf("INFO", "WARN", "DEBUG", "ERROR", "UNKNOWN", "KAFKA")
                                                        val counts = levels.map { levelCounts.getOrDefault(it, 0) }
                                                        
                                                        timePoints.add(TimePointData(windowStart, counts))
                                                    }
                                                    
                                                    val maxCount = timePoints.flatMap { it.counts }.maxOrNull() ?: 1
                                                    val chartData = LogLevelChartData(timePoints, listOf("INFO", "WARN", "DEBUG", "ERROR", "UNKNOWN", "KAFKA"), maxCount)
                                                    
                                                    safeSendWithLock(json.encodeToString(
                                                        WebSocketMessage.serializer(),
                                                        WebSocketMessage("chartData", chartData = chartData)
                                                    ))
                                                }

                                            } catch (e: Exception) {
                                                if (e !is CancellationException) {
                                                    println("Error processing kafka message for client $clientId: ${e.message}")
                                                }
                                            }
                                        }

                                        // Process log cluster updates
                                        val clusterMessage = Channels.logClusterCmdGuiChannel.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS)
                                        if (clusterMessage is LogClusterList) {
                                            try {
                                                val clusters = clusterMessage.clusters.map { cluster ->
                                                    LogClusterInfo(
                                                        count = cluster.count.toInt(),
                                                        level = cluster.level.name,
                                                        indexIdentifier = cluster.indexIdentifier,
                                                        messagePattern = cluster.block
                                                    )
                                                }

                                                safeSendWithLock(json.encodeToString(
                                                    WebSocketMessage.serializer(),
                                                    WebSocketMessage("logClusters", logClusters = clusters)
                                                ))
                                            } catch (e: Exception) {
                                                if (e !is CancellationException) {
                                                    println("Error processing log cluster message for client $clientId: ${e.message}")
                                                }
                                            }
                                        }

                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Exception) {
                                        println("Error in data processing for client $clientId: ${e.message}")
                                    }
                                    yield()
                                }
                            } catch (e: CancellationException) {
                                // Don't log cancellation as error - it's expected during cleanup
                            } catch (e: Exception) {
                                println("Data processing job error for client $clientId: ${e.message}")
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
                                                        safeSendWithLock(json.encodeToString(
                                                            WebSocketMessage.serializer(),
                                                            WebSocketMessage("pods", podMaps = podMaps)
                                                        ))
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
                                                            safeSendWithLock(json.encodeToString(
                                                                WebSocketMessage.serializer(),
                                                                WebSocketMessage("topics", topics = topicList)
                                                            ))
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
                                                            safeSendWithLock(json.encodeToString(
                                                                WebSocketMessage.serializer(),
                                                                WebSocketMessage("lagInfo", lagInfo = lagInfoList)
                                                            ))
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
                        dataProcessingJob.join()

                    } catch (e: Exception) {
                        println("Error in WebSocket session for client $clientId: ${e.message}")
                    } finally {
                        // Clean up client-specific resources
                        clients.remove(clientId)
                        clientLocks.remove(clientId)
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