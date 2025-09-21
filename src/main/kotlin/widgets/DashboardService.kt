package widgets

import app.*
import kafka.Kafka
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs

/**
 * Service for providing dashboard metrics interface (thread-safe, idiomatic Kotlin)
 * Data collection and calculations are handled by DataCollector
 */
class DashboardService private constructor() {

    companion object {
        @Volatile
        private var instance: DashboardService? = null

        fun getInstance(): DashboardService =
            instance ?: synchronized(this) {
                instance ?: DashboardService().also { instance = it }
            }

        // The only allowed/supported minute intervals in DataCollector
        private val SUPPORTED_MINUTES = listOf(5, 15, 60, 360, 1440)

        // Snap UI-selected minutes to the closest supported interval
        private fun normalizeMinutes(minutes: Int): Int =
            SUPPORTED_MINUTES.minBy { abs(it - minutes) }
    }

    // Data collector instance
    private val dataCollector = DataCollector.getInstance()

    // Instant update callback
    private var onDataUpdateCallback: (() -> Unit)? = null

    // Selected UI range (always stored as a supported interval)
    private val selectedRangeMinutes = AtomicInteger(5)

    data class DashboardMetrics(
        val totalMessages: Long,
        val averageThroughput: Double,
        val errorRate: Double,
        val activePods: Int,
        val kafkaLag: Map<String, Kafka.LagInfo>
    )

    fun setOnDataUpdateCallback(callback: () -> Unit) {
        onDataUpdateCallback = callback
        dataCollector.setOnDataUpdateCallback(callback)
    }

    fun setSelectedTimeRange(minutes: Int) {
        selectedRangeMinutes.set(normalizeMinutes(minutes))
    }

    private fun selectedMinutes(): Int = selectedRangeMinutes.get()

    fun recordLogMessage(line: DomainLine) {
        dataCollector.recordLogMessage(line)
        onDataUpdateCallback?.invoke()
    }

    fun recordLogMessageWithTimestamp(line: DomainLine) {
        dataCollector.recordLogMessageWithTimestamp(line)
    }

    fun updateKafkaLag(lagInfo: List<Kafka.LagInfo>) {
        dataCollector.updateKafkaLag(lagInfo)
    }

    fun registerActivePod(podName: String) {
        dataCollector.registerActivePod(podName)
    }

    fun unregisterActivePod(podName: String) {
        dataCollector.unregisterActivePod(podName)
    }

    fun getActivePodsCount(): Int = dataCollector.getActivePodsCount()
    fun getActivePods(): Set<String> = dataCollector.getActivePods()

    fun getDashboardMetrics(): DashboardMetrics {
        val minutes = selectedMinutes()
        val (total, errors, avg) = dataCollector.getAggregatedMetricsFromBuckets(minutes)
        val errorRate = if (total > 0) (errors.toDouble() / total) * 100 else 0.0

        return DashboardMetrics(
            totalMessages = total,
            averageThroughput = avg,
            errorRate = errorRate,
            activePods = getActivePodsCount(),
            kafkaLag = dataCollector.getKafkaLagData()
        )
    }

    // If caller passes an arbitrary minutes value, normalize it to supported intervals
    fun getLogLevelData(minutes: Int = selectedMinutes()): List<DataCollector.LogLevelDataPoint> {
        val m = normalizeMinutes(minutes)
        return dataCollector.getLogLevelData(m)
    }

    fun getPodThroughputRates(minutes: Int = selectedMinutes()): Map<String, Double> {
        val m = normalizeMinutes(minutes)
        return dataCollector.getPodThroughputRates(m)
    }

    fun getCurrentPodThroughput(): Map<String, Double> {
        // Uses last ~60 seconds internally from 5-minute bucket in DataCollector
        return dataCollector.getCurrentPodThroughput()
    }

    fun getHighResThroughputData(seconds: Int = 60): List<DataCollector.ThroughputDataPoint> {
        // High-res is always from the 5-minute bucket in DataCollector
        return dataCollector.getHighResThroughputData(seconds)
    }

    fun getTimeBucketedThroughputData(
        minutes: Int = selectedMinutes(),
        bucketSizeSeconds: Int = 10
    ): List<DataCollector.ThroughputDataPoint> {
        val m = normalizeMinutes(minutes)
        return dataCollector.getTimeBucketedThroughputData(m, bucketSizeSeconds)
    }

    fun getKafkaLagSummary(): Map<String, Long> {
        return dataCollector.getKafkaLagSummary()
    }

    fun getPartitionLagDetails(hideZeroLag: Boolean = true): List<Map<String, Any>> {
        return dataCollector.getPartitionLagDetails(hideZeroLag)
    }

    fun shutdown() {
        dataCollector.shutdown()
    }
}