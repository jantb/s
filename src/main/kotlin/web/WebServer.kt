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
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.seconds
import kafka.Kafka

class WebServer(private val port: Int = 9999) {
    private val clients = ConcurrentHashMap<String, WebSocketSession>()
    private val clientPods = ConcurrentHashMap<String, MutableSet<String>>()
    private val clientChannels = ConcurrentHashMap<String, Channel<String>>()
    private val clientTopics = ConcurrentHashMap<String, MutableSet<String>>()
    private val clientMutex = Mutex()
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
                    
                    // Initialize client state atomically
                    clientMutex.withLock {
                        clients[clientId] = this
                        clientPods[clientId] = mutableSetOf()
                        clientTopics[clientId] = mutableSetOf()
                        
                        // Create a dedicated channel for this client with buffering
                        val clientChannel = Channel<String>(capacity = Channel.BUFFERED)
                        clientChannels[clientId] = clientChannel
                    }
                    
                    val clientChannel = clientChannels[clientId]!!

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

                        // Safe send function using channel-based approach
                        suspend fun safeSend(message: String): Boolean {
                            return try {
                                clientChannel.trySend(message).isSuccess
                            } catch (e: Exception) {
                                false
                            }
                        }

                        // Use structured concurrency with coroutineScope
                        coroutineScope {
                            // Channel sender job - handles outgoing messages
                            launch {
                                try {
                                    for (message in clientChannel) {
                                        if (isActive && !incoming.isClosedForReceive) {
                                            withTimeout(2000) {
                                                send(Frame.Text(message))
                                            }
                                        } else {
                                            break
                                        }
                                    }
                                } catch (e: CancellationException) {
                                    // Expected during cleanup
                                } catch (e: Exception) {
                                    println("Error sending message for client $clientId: ${e.message}")
                                }
                            }

                            // Stats job
                            launch {
                                try {
                                    while (isActive) {
                                        safeSend(json.encodeToString(
                                            StatsMessage.serializer(),
                                            StatsMessage(State.indexedLines.get())
                                        ))
                                        delay(1000)
                                    }
                                } catch (e: CancellationException) {
                                    // Expected during cleanup
                                }
                            }

                            // Data processing job using proper channel consumption
                            launch {
                                try {
                                    while (isActive) {
                                        // Check for kafka command GUI messages
                                        val kafkaMessage = Channels.kafkaCmdGuiChannel.poll()
                                        if (kafkaMessage is ResultChanged) {
                                            // Process log data only
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
                                                safeSend(json.encodeToString(
                                                    WebSocketMessage.serializer(),
                                                    WebSocketMessage("logs", logs = logs)
                                                ))
                                            }
                                        }

                                        // Check for log cluster messages
                                        val clusterMessage = Channels.logClusterCmdGuiChannel.poll()
                                        if (clusterMessage is LogClusterList) {
                                            val clusters = clusterMessage.clusters.map { cluster ->
                                                LogClusterInfo(
                                                    count = cluster.count.toInt(),
                                                    level = cluster.level.name,
                                                    indexIdentifier = cluster.indexIdentifier,
                                                    messagePattern = cluster.block
                                                )
                                            }

                                            safeSend(json.encodeToString(
                                                WebSocketMessage.serializer(),
                                                WebSocketMessage("logClusters", logClusters = clusters)
                                            ))
                                        }
                                        
                                        // Small delay to prevent busy waiting
                                        delay(50)
                                    }
                                } catch (e: CancellationException) {
                                    // Expected during cleanup
                                }
                            }

                            // Message handler job
                            launch {
                                try {
                                    for (frame in incoming) {
                                        when (frame) {
                                            is Frame.Text -> {
                                                try {
                                                    val message = json.decodeFromString<ClientMessage>(frame.readText())
                                                    when (message.action) {
                                                        "listPods" -> {
                                                            val listPods = ListPods()
                                                            Channels.podsChannel.offer(listPods)
                                                            val pods = listPods.result.get()
                                                            val podMaps = pods.map { pod ->
                                                                mapOf(
                                                                    "name" to pod.name,
                                                                    "version" to pod.version,
                                                                    "creationTimestamp" to pod.creationTimestamp
                                                                )
                                                            }
                                                            safeSend(json.encodeToString(
                                                                WebSocketMessage.serializer(),
                                                                WebSocketMessage("pods", podMaps = podMaps)
                                                            ))
                                                        }
                                                        "listenPod" -> {
                                                            message.podName?.let {
                                                                Channels.podsChannel.offer(ListenToPod(it))
                                                                clientMutex.withLock {
                                                                    clientPods[clientId]?.add(it)
                                                                }
                                                            }
                                                        }
                                                        "unlistenPod" -> {
                                                            message.podName?.let {
                                                                Channels.podsChannel.offer(UnListenToPod(it))
                                                                clientMutex.withLock {
                                                                    clientPods[clientId]?.remove(it)
                                                                }
                                                            }
                                                        }
                                                        "search" -> {
                                                            message.query?.let {
                                                                try {
                                                                    Channels.searchChannel.trySend(
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
                                                            // Respond to ping to keep connection alive
                                                        }
                                                        "listTopics" -> {
                                                            val listTopics = ListTopics()
                                                            Channels.kafkaChannel.offer(listTopics)
                                                            try {
                                                                val topics = listTopics.result.get()
                                                                val topicList = topics.map { KafkaTopic(it) }
                                                                safeSend(json.encodeToString(
                                                                    WebSocketMessage.serializer(),
                                                                    WebSocketMessage("topics", topics = topicList)
                                                                ))
                                                            } catch (e: Exception) {
                                                                println("Error listing Kafka topics for client $clientId: ${e.message}")
                                                            }
                                                        }
                                                        "listenToTopics" -> {
                                                            message.topics?.let { topics ->
                                                                Channels.kafkaChannel.offer(ListenToTopic(topics))
                                                                clientMutex.withLock {
                                                                    clientTopics[clientId]?.addAll(topics)
                                                                }
                                                            }
                                                        }
                                                        "unlistenToTopics" -> {
                                                            Channels.kafkaChannel.offer(UnListenToTopics)
                                                            clientMutex.withLock {
                                                                clientTopics[clientId]?.clear()
                                                            }
                                                        }
                                                        
                                                        "unassignTopics" -> {
                                                            message.topics?.let { topics ->
                                                                Channels.kafkaChannel.offer(UnassignTopics(topics))
                                                                clientMutex.withLock {
                                                                    topics.forEach { topic ->
                                                                        clientTopics[clientId]?.remove(topic)
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        "listLag" -> {
                                                            try {
                                                                val msg = ListLag()
                                                                Channels.kafkaChannel.offer(msg)
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
                                                                safeSend(json.encodeToString(
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
                                                // Handle other frame types if needed
                                            }
                                        }
                                    }
                                } catch (e: CancellationException) {
                                    // Expected during cleanup
                                } catch (e: Exception) {
                                    println("Error in message handler for client $clientId: ${e.message}")
                                }
                            }
                        }

                    } catch (e: Exception) {
                        println("Error in WebSocket session for client $clientId: ${e.message}")
                    } finally {
                        // Clean up client-specific resources atomically
                        clientMutex.withLock {
                            clients.remove(clientId)
                            clientChannels.remove(clientId)?.close()
                            
                            // Stop listening to any pods this client was monitoring
                            clientPods[clientId]?.forEach { podName ->
                                Channels.podsChannel.offer(UnListenToPod(podName))
                            }
                            clientPods.remove(clientId)
                            
                            // Stop listening to any Kafka topics this client was monitoring
                            clientTopics[clientId]?.let { topics ->
                                if (topics.isNotEmpty()) {
                                    Channels.kafkaChannel.offer(UnListenToTopics)
                                }
                            }
                            clientTopics.remove(clientId)
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
