package widgets

import LogLevel
import app.DomainLine
import app.LogLineDomain
import app.KafkaLineDomain
import app.Channels
import app.ListLag
import kafka.Kafka
import kotlinx.coroutines.*
import kotlinx.coroutines.time.withTimeout
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes

/**
 * Service for collecting and providing dashboard metrics
 */
class DashboardService {

    companion object {
        @Volatile
        private var instance: DashboardService? = null

        fun getInstance(): DashboardService {
            return instance ?: synchronized(this) {
                instance ?: DashboardService().also { instance = it }
            }
        }
    }

    // Metrics storage
    private val podThroughput = ConcurrentHashMap<String, AtomicLong>()
    private val queueThroughput = ConcurrentHashMap<String, AtomicLong>()
    private val logLevelCounts = ConcurrentHashMap<LogLevel, AtomicLong>()
    private val kafkaLagData = ConcurrentHashMap<String, Kafka.LagInfo>()
    private val activePods = ConcurrentHashMap<String, Boolean>()

    // Historical data for charts
    private val throughputHistory = mutableListOf<ThroughputDataPoint>()
    private val logLevelHistory = mutableListOf<LogLevelDataPoint>()

    // Configuration
    private val maxHistorySize = 360 // Keep 1 hour of data at 10-second intervals
    private val updateInterval = 10.seconds

    // Coroutine scope for background tasks
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    data class ThroughputDataPoint(
        val timestamp: Instant,
        val podThroughput: Map<String, Long>,
        val queueThroughput: Map<String, Long>
    )

    data class LogLevelDataPoint(
        val timestamp: Instant,
        val counts: Map<LogLevel, Long>
    )

    data class DashboardMetrics(
        val totalMessages: Long,
        val averageThroughput: Double,
        val errorRate: Double,
        val activePods: Int,
        val kafkaLag: Map<String, Kafka.LagInfo>
    )

    init {
        startMetricsCollection()
    }

    /**
     * Record a log message for metrics calculation
     */
    fun recordLogMessage(logLine: DomainLine) {
        // Update log level counts
        logLevelCounts.getOrPut(logLine.level) { AtomicLong(0) }
            .incrementAndGet()

        // Extract pod information based on DomainLine type
        when (logLine) {
            is LogLineDomain -> {
                // Use serviceName as pod identifier
                val podName = logLine.serviceName
                podThroughput.getOrPut(podName) { AtomicLong(0) }
                    .incrementAndGet()
            }
            is KafkaLineDomain -> {
                // Use topic as queue identifier
                val topic = logLine.topic
                queueThroughput.getOrPut(topic) { AtomicLong(0) }
                    .incrementAndGet()
            }
        }
    }

    /**
     * Update Kafka lag information
     */
    fun updateKafkaLag(lagInfo: List<Kafka.LagInfo>) {
        lagInfo.forEach { info ->
            kafkaLagData["${info.groupId}-${info.topic}-${info.partition}"] = info
        }
    }

    /**
     * Register an active pod (being listened to)
     */
    fun registerActivePod(podName: String) {
        activePods[podName] = true
    }

    /**
     * Unregister an active pod (no longer being listened to)
     */
    fun unregisterActivePod(podName: String) {
        activePods.remove(podName)
    }

    /**
     * Get the count of all active pods (both with throughput and actively listened to)
     */
    fun getActivePodsCount(): Int {
        val podsWithThroughput = podThroughput.keys
        val activelyListenedPods = activePods.keys
        return (podsWithThroughput + activelyListenedPods).size
    }

    /**
     * Get current dashboard metrics (cumulative since startup)
     */
    fun getDashboardMetrics(): DashboardMetrics {
        val totalMessages = logLevelCounts.values.sumOf { it.get() }
        val totalErrors = logLevelCounts[LogLevel.ERROR]?.get() ?: 0
        val errorRate = if (totalMessages > 0) (totalErrors.toDouble() / totalMessages) * 100 else 0.0

        // Calculate average throughput over last 5 minutes
        val fiveMinutesAgo = Clock.System.now().minus(5.minutes)
        val recentThroughput = throughputHistory
            .filter { it.timestamp > fiveMinutesAgo }
            .sumOf { it.podThroughput.values.sum() + it.queueThroughput.values.sum() }

        val averageThroughput = if (throughputHistory.isNotEmpty()) {
            recentThroughput / (throughputHistory.size * 10.0) // per second
        } else 0.0

        return DashboardMetrics(
            totalMessages = totalMessages,
            averageThroughput = averageThroughput,
            errorRate = errorRate,
            activePods = getActivePodsCount(),
            kafkaLag = kafkaLagData.toMap()
        )
    }

    /**
     * Get current interval metrics (resets every 10 seconds)
     */
    fun getCurrentIntervalMetrics(): DashboardMetrics {
        val totalMessages = logLevelCounts.values.sumOf { it.get() }
        val totalErrors = logLevelCounts[LogLevel.ERROR]?.get() ?: 0
        val errorRate = if (totalMessages > 0) (totalErrors.toDouble() / totalMessages) * 100 else 0.0

        // Calculate average throughput for current interval
        val averageThroughput = if (totalMessages > 0) {
            totalMessages / 10.0 // per second for current 10-second interval
        } else 0.0

        return DashboardMetrics(
            totalMessages = totalMessages,
            averageThroughput = averageThroughput,
            errorRate = errorRate,
            activePods = getActivePodsCount(),
            kafkaLag = kafkaLagData.toMap()
        )
    }

    /**
     * Get throughput data for charts
     */
    fun getThroughputData(minutes: Int = 5): List<ThroughputDataPoint> {
        val cutoffTime = Clock.System.now().minus(minutes.seconds)
        return throughputHistory.filter { it.timestamp > cutoffTime }
    }

    /**
     * Get log level distribution data
     */
    fun getLogLevelData(minutes: Int = 5): List<LogLevelDataPoint> {
        val cutoffTime = Clock.System.now().minus(minutes.seconds)
        return logLevelHistory.filter { it.timestamp > cutoffTime }
    }

    /**
     * Get Kafka lag summary by severity
     */
    fun getKafkaLagSummary(): Map<String, Long> {
        val lagData = kafkaLagData.values
        return mapOf(
            "high" to lagData.filter { it.lag >= 100 }.sumOf { it.lag },
            "medium" to lagData.filter { it.lag in 10..99 }.sumOf { it.lag },
            "low" to lagData.filter { it.lag in 1..9 }.sumOf { it.lag }
        )
    }

    /**
     * Get partition lag details
     */
    fun getPartitionLagDetails(hideZeroLag: Boolean = true): List<Map<String, Any>> {
        return kafkaLagData.values
            .filter { !hideZeroLag || it.lag > 0 }
            .map {
                mapOf(
                    "topic" to it.topic,
                    "partition" to it.partition,
                    "lag" to it.lag,
                    "groupId" to it.groupId
                )
            }
            .sortedByDescending { it["lag"] as Long }
    }

    private fun startMetricsCollection() {
        scope.launch {
            while (isActive) {
                try {
                    collectMetricsSnapshot()
                    collectKafkaLag()
                    delay(updateInterval)
                } catch (e: Exception) {
                    // Log error but continue running
                    println("Error collecting metrics: ${e.message}")
                }
            }
        }
    }

    private fun collectMetricsSnapshot() {
        val now = Clock.System.now()

        // Collect throughput snapshot
        val podSnapshot = podThroughput.mapValues { it.value.get() }
        val queueSnapshot = queueThroughput.mapValues { it.value.get() }

        throughputHistory.add(ThroughputDataPoint(
            timestamp = now,
            podThroughput = podSnapshot,
            queueThroughput = queueSnapshot
        ))

        // Collect log level snapshot
        val levelSnapshot = logLevelCounts.mapValues { it.value.get() }
        logLevelHistory.add(LogLevelDataPoint(
            timestamp = now,
            counts = levelSnapshot
        ))

        // Trim history if it gets too large
        if (throughputHistory.size > maxHistorySize) {
            throughputHistory.removeFirst()
        }
        if (logLevelHistory.size > maxHistorySize) {
            logLevelHistory.removeFirst()
        }

        // Don't reset counters - keep cumulative data for dashboard metrics
        // Only reset interval counters if needed for other purposes
    }

    private fun collectKafkaLag() {
        try {
            // Use the Kafka channel to request lag information
            val lagRequest = ListLag()
            Channels.kafkaChannel.put(lagRequest)

            // Wait for the response with timeout to avoid hanging
            runBlocking {
                withTimeout(5.seconds) {
                    val lagInfo = lagRequest.result.await()
                    updateKafkaLag(lagInfo)
                }
            }
        } catch (e: Exception) {
            // Log error but don't crash the metrics collection
            println("Error collecting Kafka lag: ${e.message}")
        }
    }

    /**
     * Clean up resources
     */
    fun shutdown() {
        scope.cancel()
    }
}