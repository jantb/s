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
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds

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

    // Historical data for charts - thread-safe collections
    private val throughputHistory = java.util.concurrent.CopyOnWriteArrayList<ThroughputDataPoint>()
    private val logLevelHistory = java.util.concurrent.CopyOnWriteArrayList<LogLevelDataPoint>()

    // Configuration
    private val maxHistorySize = 8640 // Keep 24 hours of data at 10-second intervals (24 * 60 * 60 / 10)
    private val updateInterval = 1.seconds // Update every second for instant updates
    private val highResUpdateInterval = 100.milliseconds // High resolution updates for real-time charts

    // Coroutine scope for background tasks
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    // Callback for instant updates
    private var onDataUpdateCallback: (() -> Unit)? = null

    // High-resolution data for real-time updates
    private val highResThroughputData = ConcurrentHashMap<String, AtomicLong>()
    private val highResLogLevelData = ConcurrentHashMap<LogLevel, AtomicLong>()
    private val lastHighResUpdate = AtomicReference<Instant>(Clock.System.now())

    // Selected time range for metrics calculation
    private var selectedTimeRangeMinutes = 5

    data class ThroughputDataPoint(
        val messageTimestamp: Instant,  // Use actual message timestamp instead of processing time
        val podThroughput: Map<String, Long>,
        val queueThroughput: Map<String, Long>
    )

    data class LogLevelDataPoint(
        val messageTimestamp: Instant,  // Use actual message timestamp instead of processing time
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
     * Set callback for instant updates
     */
    fun setOnDataUpdateCallback(callback: () -> Unit) {
        onDataUpdateCallback = callback
    }

    /**
     * Set the selected time range for metrics calculation
     */
    fun setSelectedTimeRange(minutes: Int) {
        selectedTimeRangeMinutes = minutes
    }

    /**
     * Record a log message for metrics calculation
     */
    fun recordLogMessage(logLine: DomainLine) {
        // Update log level counts
        logLevelCounts.getOrPut(logLine.level) { AtomicLong(0) }
            .incrementAndGet()

        // Update high-resolution data for instant updates
        highResLogLevelData.getOrPut(logLine.level) { AtomicLong(0) }
            .incrementAndGet()

        // Extract pod information based on DomainLine type
        when (logLine) {
            is LogLineDomain -> {
                // Use serviceName as pod identifier
                val podName = logLine.serviceName
                podThroughput.getOrPut(podName) { AtomicLong(0) }
                    .incrementAndGet()
                highResThroughputData.getOrPut(podName) { AtomicLong(0) }
                    .incrementAndGet()
            }
            is KafkaLineDomain -> {
                // Use topic as queue identifier
                val topic = logLine.topic
                queueThroughput.getOrPut(topic) { AtomicLong(0) }
                    .incrementAndGet()
                highResThroughputData.getOrPut(topic) { AtomicLong(0) }
                    .incrementAndGet()
            }
        }

        // Trigger instant update callback
        onDataUpdateCallback?.invoke()
    }

    /**
     * Record a log message with its actual timestamp for historical data
     */
    fun recordLogMessageWithTimestamp(logLine: DomainLine) {
        val messageTime = Instant.fromEpochMilliseconds(logLine.timestamp)

        // Create a snapshot of current counters at the message timestamp
        val podSnapshot = podThroughput.mapValues { it.value.get() }
        val queueSnapshot = queueThroughput.mapValues { it.value.get() }

        throughputHistory.add(ThroughputDataPoint(
            messageTimestamp = messageTime,
            podThroughput = podSnapshot,
            queueThroughput = queueSnapshot
        ))

        // Create log level snapshot at message timestamp
        val levelSnapshot = logLevelCounts.mapValues { it.value.get() }
        logLevelHistory.add(LogLevelDataPoint(
            messageTimestamp = messageTime,
            counts = levelSnapshot
        ))
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
     * Get the set of all active pods (both with throughput and actively listened to)
     */
    fun getActivePods(): Set<String> {
        val podsWithThroughput = podThroughput.keys
        val activelyListenedPods = activePods.keys
        return (podsWithThroughput + activelyListenedPods).toSet()
    }

    /**
     * Get current dashboard metrics (cumulative since startup)
     */
    fun getDashboardMetrics(): DashboardMetrics {
        val totalMessages = logLevelCounts.values.sumOf { it.get() }
        val totalErrors = logLevelCounts[LogLevel.ERROR]?.get() ?: 0
        val errorRate = if (totalMessages > 0) (totalErrors.toDouble() / totalMessages) * 100 else 0.0

        // Calculate average throughput over the selected time range using actual message timestamps
        val selectedTimeAgo = Clock.System.now().minus(selectedTimeRangeMinutes.minutes)
        val recentData = throughputHistory.filter { it.messageTimestamp > selectedTimeAgo }

        val averageThroughput = if (recentData.size >= 2) {
            // Get the first and last data points in the time range
            val sortedData = recentData.sortedBy { it.messageTimestamp }
            val firstPoint = sortedData.first()
            val lastPoint = sortedData.last()

            // Calculate the difference in total messages between last and first point
            val firstTotal = firstPoint.podThroughput.values.sum() + firstPoint.queueThroughput.values.sum()
            val lastTotal = lastPoint.podThroughput.values.sum() + lastPoint.queueThroughput.values.sum()
            val messageDifference = lastTotal - firstTotal

            // Calculate time span between first and last point
            val timeSpanSeconds = (lastPoint.messageTimestamp - firstPoint.messageTimestamp).inWholeSeconds

            if (timeSpanSeconds > 0) {
                messageDifference.toDouble() / timeSpanSeconds
            } else {
                // Fallback to simple calculation if no time span
                messageDifference.toDouble() / selectedTimeRangeMinutes / 60.0
            }
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

        // Calculate average throughput for current interval using actual time span
        val now = Clock.System.now()
        val tenSecondsAgo = now.minus(10.seconds)
        val recentData = throughputHistory.filter { it.messageTimestamp > tenSecondsAgo }

        val averageThroughput = if (recentData.size >= 2) {
            // Get the first and last data points in the time range
            val sortedData = recentData.sortedBy { it.messageTimestamp }
            val firstPoint = sortedData.first()
            val lastPoint = sortedData.last()

            // Calculate the difference in total messages between last and first point
            val firstTotal = firstPoint.podThroughput.values.sum() + firstPoint.queueThroughput.values.sum()
            val lastTotal = lastPoint.podThroughput.values.sum() + lastPoint.queueThroughput.values.sum()
            val messageDifference = lastTotal - firstTotal

            // Calculate time span between first and last point
            val timeSpanSeconds = (lastPoint.messageTimestamp - firstPoint.messageTimestamp).inWholeSeconds

            if (timeSpanSeconds > 0) {
                messageDifference.toDouble() / timeSpanSeconds
            } else {
                // Fallback to simple calculation if no time span
                messageDifference / 10.0
            }
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
        return throughputHistory.filter { it.messageTimestamp > cutoffTime }
    }

    /**
     * Get log level distribution data
     */
    fun getLogLevelData(minutes: Int = 5): List<LogLevelDataPoint> {
        val cutoffTime = Clock.System.now().minus(minutes.seconds)
        return logLevelHistory.filter { it.messageTimestamp > cutoffTime }
    }

    /**
      * Get pod-specific throughput data for individual pod charts
      */
     fun getPodThroughputData(minutes: Int = 5): Map<String, List<Pair<Instant, Long>>> {
         val cutoffTime = Clock.System.now().minus(minutes.seconds)
         val relevantHistory = throughputHistory.filter { it.messageTimestamp > cutoffTime }

         val podData = mutableMapOf<String, MutableList<Pair<Instant, Long>>>()

         // Initialize all active pods
         (podThroughput.keys + activePods.keys).forEach { podName ->
             podData[podName] = mutableListOf()
         }

         // Build time series data for each pod
         relevantHistory.forEach { dataPoint ->
             dataPoint.podThroughput.forEach { (podName, throughput) ->
                 podData.getOrPut(podName) { mutableListOf() }.add(dataPoint.messageTimestamp to throughput)
             }
         }

         return podData
     }

    /**
      * Get pod throughput rates (messages per second) for different time ranges
      */
     fun getPodThroughputRates(minutes: Int = 5): Map<String, Double> {
         val cutoffTime = Clock.System.now().minus(minutes.seconds)
         val relevantHistory = throughputHistory.filter { it.messageTimestamp > cutoffTime }

         if (relevantHistory.size < 2) {
             return emptyMap()
         }

         // Get the first and last data points in the time range
         val sortedData = relevantHistory.sortedBy { it.messageTimestamp }
         val firstPoint = sortedData.first()
         val lastPoint = sortedData.last()

         // Calculate time span between first and last point
         val timeSpanSeconds = (lastPoint.messageTimestamp - firstPoint.messageTimestamp).inWholeSeconds

         if (timeSpanSeconds <= 0) {
             return emptyMap()
         }

         // Calculate per-pod throughput rates
         val result = mutableMapOf<String, Double>()
         lastPoint.podThroughput.forEach { (podName, lastCount) ->
             val firstCount = firstPoint.podThroughput[podName] ?: 0L
             val messageDifference = lastCount - firstCount
             result[podName] = messageDifference.toDouble() / timeSpanSeconds
         }

         return result
     }

    /**
     * Get current pod throughput rates (messages per second) - high resolution
     */
    fun getCurrentPodThroughput(): Map<String, Double> {
        val now = Clock.System.now()
        val lastUpdate = lastHighResUpdate.get()

        // Calculate time since last high-res update
        val timeSinceUpdate = now.minus(lastUpdate)
        val secondsSinceUpdate = timeSinceUpdate.inWholeMilliseconds / 1000.0

        if (secondsSinceUpdate < 0.1) {
            // Very recent data, use high-res counters
            return highResThroughputData.mapValues { (_, counter) ->
                counter.get() / secondsSinceUpdate.coerceAtLeast(0.1)
            }
        } else {
            // Fall back to historical data for longer periods
            val oneMinuteAgo = now.minus(1.minutes)
            val relevantHistory = throughputHistory.filter { it.messageTimestamp > oneMinuteAgo }

            if (relevantHistory.size >= 2) {
                // Get the first and last data points in the time range
                val sortedData = relevantHistory.sortedBy { it.messageTimestamp }
                val firstPoint = sortedData.first()
                val lastPoint = sortedData.last()

                // Calculate the difference for each pod between last and first point
                val result = mutableMapOf<String, Double>()
                val timeSpanSeconds = (lastPoint.messageTimestamp - firstPoint.messageTimestamp).inWholeSeconds

                if (timeSpanSeconds > 0) {
                    // Calculate per-pod throughput rates
                    lastPoint.podThroughput.forEach { (podName, lastCount) ->
                        val firstCount = firstPoint.podThroughput[podName] ?: 0L
                        val messageDifference = lastCount - firstCount
                        result[podName] = messageDifference.toDouble() / timeSpanSeconds
                    }
                } else {
                    // Fallback if no time span
                    lastPoint.podThroughput.forEach { (podName, lastCount) ->
                        val firstCount = firstPoint.podThroughput[podName] ?: 0L
                        val messageDifference = lastCount - firstCount
                        result[podName] = messageDifference / 60.0
                    }
                }

                return result
            } else {
                // Not enough data points
                return emptyMap()
            }
        }
    }

    /**
     * Get high-resolution throughput data for real-time charts
     */
    fun getHighResThroughputData(seconds: Int = 60): List<ThroughputDataPoint> {
        val cutoffTime = Clock.System.now().minus(seconds.seconds)
        return throughputHistory.filter { it.messageTimestamp > cutoffTime }
    }

    /**
     * Get time-bucketed data for different time ranges
     */
    fun getTimeBucketedThroughputData(minutes: Int, bucketSizeSeconds: Int = 10): List<ThroughputDataPoint> {
        val cutoffTime = Clock.System.now().minus(minutes.minutes)
        val rawData = throughputHistory.filter { it.messageTimestamp > cutoffTime }

        if (rawData.isEmpty()) return emptyList()

        // Group data into buckets
        val buckets = mutableMapOf<Instant, MutableMap<String, Long>>()

        rawData.forEach { dataPoint ->
            // Round message timestamp to bucket boundary
            val bucketStart = dataPoint.messageTimestamp.minus(
                (dataPoint.messageTimestamp.epochSeconds % bucketSizeSeconds).seconds
            )

            buckets.getOrPut(bucketStart) { mutableMapOf() }.apply {
                dataPoint.podThroughput.forEach { (pod, count) ->
                    this[pod] = this.getOrDefault(pod, 0L) + count
                }
                dataPoint.queueThroughput.forEach { (queue, count) ->
                    this["queue_$queue"] = this.getOrDefault("queue_$queue", 0L) + count
                }
            }
        }

        return buckets.map { (timestamp, data) ->
            ThroughputDataPoint(
                messageTimestamp = timestamp,
                podThroughput = data.filterKeys { !it.startsWith("queue_") },
                queueThroughput = data.filterKeys { it.startsWith("queue_") }
                    .mapKeys { it.key.removePrefix("queue_") }
            )
        }.sortedBy { it.messageTimestamp }
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
            messageTimestamp = now,
            podThroughput = podSnapshot,
            queueThroughput = queueSnapshot
        ))

        // Collect log level snapshot
        val levelSnapshot = logLevelCounts.mapValues { it.value.get() }
        logLevelHistory.add(LogLevelDataPoint(
            messageTimestamp = now,
            counts = levelSnapshot
        ))

        // Reset high-resolution counters for next interval
        highResThroughputData.values.forEach { it.set(0) }
        highResLogLevelData.values.forEach { it.set(0) }
        lastHighResUpdate.set(now)

        // Trim history if it gets too large - keep 24 hours of data
        val cutoffTime = now.minus(24.hours)
        throughputHistory.removeIf { it.messageTimestamp < cutoffTime }
        logLevelHistory.removeIf { it.messageTimestamp < cutoffTime }

        // Additional pruning: keep only maxHistorySize entries as backup
        while (throughputHistory.size > maxHistorySize) {
            throughputHistory.removeFirst()
        }
        while (logLevelHistory.size > maxHistorySize) {
            logLevelHistory.removeFirst()
        }
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