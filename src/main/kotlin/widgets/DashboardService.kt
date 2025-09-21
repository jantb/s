package widgets

import app.DomainLine
import kafka.Kafka
import kotlinx.datetime.Instant
import java.util.concurrent.atomic.AtomicLong

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
    }

    // Data collector instance
    private val dataCollector = DataCollector.getInstance()

    // Instant update callback
    private var onDataUpdateCallback: (() -> Unit)? = null

    // Selected UI range
    private val selectedRangeMinutes = AtomicLong(5)

    data class DashboardMetrics(
        val totalMessages: Long,
        val averageThroughput: Double,
        val errorRate: Double,
        val activePods: Int,
        val kafkaLag: Map<String, Kafka.LagInfo>
    )

    init {
        // No initialization needed - DataCollector handles its own lifecycle
    }

    fun setOnDataUpdateCallback(callback: () -> Unit) {
        onDataUpdateCallback = callback
        // Also set callback on data collector to ensure updates are propagated
        dataCollector.setOnDataUpdateCallback(callback)
    }

    fun setSelectedTimeRange(minutes: Int) {
        selectedRangeMinutes.set(minutes.toLong())
    }

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
        val minutes = selectedRangeMinutes.get().toInt()
        val (total, errors, avg) = when (minutes) {
            5 -> dataCollector.getRawMetricsFromBuckets()
            15, 60, 360, 1440 -> dataCollector.getAggregatedMetricsFromBuckets(minutes)
            else -> dataCollector.getMetricsFromHistory(minutes)
        }

        val errorRate = if (total > 0) (errors.toDouble() / total) * 100 else 0.0
        return DashboardMetrics(
            totalMessages = total,
            averageThroughput = avg,
            errorRate = errorRate,
            activePods = getActivePodsCount(),
            kafkaLag = dataCollector.getKafkaLagData()
        )
    }

    fun getLogLevelData(minutes: Int = 5): List<DataCollector.LogLevelDataPoint> {
        return dataCollector.getLogLevelData(minutes)
    }


    fun getPodThroughputRates(minutes: Int = 5): Map<String, Double> {
        return dataCollector.getPodThroughputRates(minutes)
    }

    fun getCurrentPodThroughput(): Map<String, Double> {
        return dataCollector.getCurrentPodThroughput()
    }

    fun getHighResThroughputData(seconds: Int = 60): List<DataCollector.ThroughputDataPoint> {
        return dataCollector.getHighResThroughputData(seconds)
    }

    fun getTimeBucketedThroughputData(minutes: Int, bucketSizeSeconds: Int = 10): List<DataCollector.ThroughputDataPoint> {
        return dataCollector.getTimeBucketedThroughputData(minutes, bucketSizeSeconds)
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