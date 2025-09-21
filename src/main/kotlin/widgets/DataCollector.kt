package widgets

import LogLevel
import app.*
import kafka.Kafka
import kotlin.math.max
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * Event-driven, bucketed data collector.
 *
 * - All calculations are done on insert (no background loops, no periodic snapshots).
 * - All datapoints (except Kafka lag) are bucketed.
 * - "Live" metrics are derived from buckets; no separate live/high-res counters.
 * - Buckets are pruned on insert by time retention and optional max size.
 *
 * Buckets:
 * - 5-minute bucket uses 10s windows (fine-grained live view).
 * - 15, 60, 360, 1440-minute buckets use full-window aggregation.
 */
@OptIn(ExperimentalTime::class)
class DataCollector private constructor() {

    companion object {
        @Volatile
        private var instance: DataCollector? = null

        fun getInstance(): DataCollector =
            instance ?: synchronized(this) {
                instance ?: DataCollector().also { instance = it }
            }
    }

    // Configuration
    private val maxBucketSize = 1000
    private val intervals = listOf(5, 15, 60, 360, 1440)
    private val bucketSpecs: Map<Int, BucketSpec> =
        intervals.associateWith { minutes ->
            BucketSpec(retention = minutes.minutes, windowSizeSeconds = 1)
        }

    private data class BucketSpec(
        val retention: Duration,
        val windowSizeSeconds: Int
    )

    // State
    // - We keep per-interval, per-window aggregates keyed by windowStart (Instant).
    // - LinkedHashMap preserves insertion order; we prune by time and bound by size.

    private val throughputBuckets: MutableMap<Int, LinkedHashMap<Instant, MutableAggregatedThroughput>> =
        intervals.associateWith { LinkedHashMap<Instant, MutableAggregatedThroughput>() }.toMutableMap()

    private val logLevelBuckets: MutableMap<Int, LinkedHashMap<Instant, MutableAggregatedLogLevel>> =
        intervals.associateWith { LinkedHashMap<Instant, MutableAggregatedLogLevel>() }.toMutableMap()

    private val activePods = mutableSetOf<String>() // explicitly registered/known pods
    private val kafkaLagData = mutableMapOf<String, Kafka.LagInfo>()

    // Thread-safety
    private val lock = Any()

    // Internal mutable aggregates (converted to immutable DTOs when reading)
    private data class MutableAggregatedThroughput(
        val start: Instant,
        val end: Instant,
        val totalPodThroughput: MutableMap<String, Long> = mutableMapOf(),
        val totalQueueThroughput: MutableMap<String, Long> = mutableMapOf(),
        var events: Int = 0
    )

    private data class MutableAggregatedLogLevel(
        val start: Instant,
        val end: Instant,
        val totalCounts: MutableMap<LogLevel, Long> = mutableMapOf(),
        var events: Int = 0
    )

    // Public immutable DTOs
    data class ThroughputDataPoint(
        val messageTimestamp: Instant, // bucket end
        val podThroughput: Map<String, Long>,
        val queueThroughput: Map<String, Long>
    )

    data class LogLevelDataPoint(
        val messageTimestamp: Instant, // bucket end
        val counts: Map<LogLevel, Long>
    )

    data class AggregatedThroughputDataPoint(
        val timeWindowStart: Instant,
        val timeWindowEnd: Instant,
        val totalPodThroughput: Map<String, Long>,
        val totalQueueThroughput: Map<String, Long>,
        val dataPointCount: Int
    )

    data class AggregatedLogLevelDataPoint(
        val timeWindowStart: Instant,
        val timeWindowEnd: Instant,
        val totalCounts: Map<LogLevel, Long>,
        val dataPointCount: Int
    )

    data class AggregationResult(
        val totalPodThroughput: Map<String, Long>,
        val totalQueueThroughput: Map<String, Long>,
        val totalLogLevelCounts: Map<LogLevel, Long>,
        val dataPointCount: Int
    )

    // Public interface ---------------------------------------------------------------------------

    fun recordLogMessage(line: DomainLine) {
        val ts = Clock.System.now()
        recordLogMessageAt(line, ts)
    }

    fun recordLogMessageWithTimestamp(line: DomainLine) {
        val ts = Instant.fromEpochMilliseconds(line.timestamp)
        recordLogMessageAt(line, ts)
    }

    fun updateKafkaLag(lagInfo: List<Kafka.LagInfo>) = synchronized(lock) {
        lagInfo.forEach { info ->
            kafkaLagData["${info.groupId}-${info.topic}-${info.partition}"] = info
        }
    }

    fun registerActivePod(podName: String) = synchronized(lock) {
        activePods += podName
    }

    fun unregisterActivePod(podName: String) = synchronized(lock) {
        activePods -= podName
    }

    fun getActivePods(): Set<String> = synchronized(lock) {
        (recentPodsFromBuckets(5) + activePods).toSet()
    }

    fun getActivePodsCount(): Int = getActivePods().size

    // Data retrieval -----------------------------------------------------------------------------

    fun getThroughputData(minutes: Int = 5): List<ThroughputDataPoint> = synchronized(lock) {
        val bucket = throughputBuckets[minutes] ?: return@synchronized emptyList()
        bucket.values
            .sortedBy { it.end }
            .map {
                ThroughputDataPoint(
                    messageTimestamp = it.end,
                    podThroughput = it.totalPodThroughput.toMap(),
                    queueThroughput = it.totalQueueThroughput.toMap()
                )
            }
    }

    fun getLogLevelData(minutes: Int = 5): List<LogLevelDataPoint> = synchronized(lock) {
        val bucket = logLevelBuckets[minutes] ?: return@synchronized emptyList()
        bucket.values
            .sortedBy { it.end }
            .map {
                LogLevelDataPoint(
                    messageTimestamp = it.end,
                    counts = it.totalCounts.toMap()
                )
            }
    }

    fun getPodThroughputData(minutes: Int = 5): Map<String, List<Pair<Instant, Long>>> {
        val history = getThroughputData(minutes)
        val pods = history.flatMap { it.podThroughput.keys }.toSet() + getActivePods()
        return pods.associateWith { pod ->
            history.mapNotNull { dp ->
                dp.podThroughput[pod]?.let { dp.messageTimestamp to it }
            }
        }
    }

    fun getPodThroughputRates(minutes: Int = 5): Map<String, Double> {
        val history = getThroughputData(minutes).sortedBy { it.messageTimestamp }
        if (history.isEmpty()) return emptyMap()

        val span = (history.last().messageTimestamp - history.first().messageTimestamp).inWholeSeconds
        if (span <= 0) return emptyMap()

        val totalsByPod = history
            .flatMap { it.podThroughput.entries }
            .groupBy({ it.key }, { it.value })
            .mapValues { (_, counts) -> counts.sum().toDouble() }

        // average rate per second by pod
        return totalsByPod.mapValues { (_, total) -> total / span }
    }

    fun getCurrentPodThroughput(): Map<String, Double> {
        val now = Clock.System.now()
        val since = now - 60.seconds
        val recent = getThroughputData(5).filter { it.messageTimestamp > since }.sortedBy { it.messageTimestamp }
        if (recent.isEmpty()) return emptyMap()

        val spanSeconds = max(
            1L,
            (recent.last().messageTimestamp - recent.first().messageTimestamp).inWholeSeconds
        )

        val totalsByPod = recent
            .flatMap { it.podThroughput.entries }
            .groupBy({ it.key }, { it.value })
            .mapValues { (_, counts) -> counts.sum().toDouble() }

        return totalsByPod.mapValues { (_, total) -> total / spanSeconds }
    }

    fun getHighResThroughputData(seconds: Int = 60): List<ThroughputDataPoint> {
        val cutoff = Clock.System.now() - seconds.seconds
        // 5-min bucket has 10s resolution; filter it
        return getThroughputData(5).filter { it.messageTimestamp > cutoff }
    }

    fun getTimeBucketedThroughputData(minutes: Int, bucketSizeSeconds: Int = 10): List<ThroughputDataPoint> {
        val raw = getThroughputData(minutes)
        if (raw.isEmpty()) return emptyList()

        val acc = mutableMapOf<Instant, MutableMap<String, Long>>()

        raw.forEach { dp ->
            val bucketStart = dp.messageTimestamp.roundDown(bucketSizeSeconds)
            val total = mutableMapOf<String, Long>()

            // combine pod and queue with a prefix to accumulate then split
            dp.podThroughput.forEach { (k, v) -> total[k] = (total[k] ?: 0) + v }
            dp.queueThroughput.forEach { (k, v) -> total["queue_$k"] = (total["queue_$k"] ?: 0) + v }

            val cur = acc.getOrPut(bucketStart) { mutableMapOf() }
            total.forEach { (k, v) -> cur[k] = (cur[k] ?: 0) + v }
        }

        return acc.entries
            .sortedBy { it.key }
            .map { (start, totals) ->
                ThroughputDataPoint(
                    messageTimestamp = start + bucketSizeSeconds.seconds,
                    podThroughput = totals.filterKeys { !it.startsWith("queue_") },
                    queueThroughput = totals
                        .filterKeys { it.startsWith("queue_") }
                        .mapKeys { it.key.removePrefix("queue_") }
                )
            }
    }

    fun getKafkaLagSummary(): Map<String, Long> = synchronized(lock) {
        val values = kafkaLagData.values
        mapOf(
            "high" to values.filter { it.lag >= 100 }.sumOf { it.lag },
            "medium" to values.filter { it.lag in 10..99 }.sumOf { it.lag },
            "low" to values.filter { it.lag in 1..9 }.sumOf { it.lag }
        )
    }

    fun getPartitionLagDetails(hideZeroLag: Boolean = true): List<Map<String, Any>> = synchronized(lock) {
        kafkaLagData.values
            .asSequence()
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
            .toList()
    }

    // Metrics (from buckets only) ----------------------------------------------------------------

    fun getRawMetricsFromBuckets(): Triple<Long, Long, Double> =
        getAggregatedMetricsFromBucketsInternal(minutes = 5)

    fun getAggregatedMetricsFromBuckets(minutes: Int): Triple<Long, Long, Double> =
        getAggregatedMetricsFromBucketsInternal(minutes)

    fun getMetricsFromHistory(minutes: Int): Triple<Long, Long, Double> =
        getAggregatedMetricsFromBucketsInternal(minutes)

    // Lifecycle ----------------------------------------------------------------------------------

    fun shutdown() {
        // No background jobs anymore. Kept for interface compatibility.
    }

    // DashboardService helpers -------------------------------------------------------------------

    // Note: these are derived from the 24h bucket (1440 minutes).
    fun getCurrentTotalMessages(): Long {
        val ll = getLogLevelData(1440)
        return ll.sumOf { it.counts.values.sum() }
    }

    fun getCurrentErrorCount(): Long {
        val ll = getLogLevelData(1440)
        return ll.sumOf { it.counts[LogLevel.ERROR] ?: 0L }
    }

    fun getCurrentAverageThroughput(): Double {
        val now = Clock.System.now()
        val since = now - 10.seconds
        val recent = getThroughputData(5).filter { it.messageTimestamp > since }.sortedBy { it.messageTimestamp }

        if (recent.isEmpty()) return 0.0
        val span = max(1L, (recent.last().messageTimestamp - recent.first().messageTimestamp).inWholeSeconds)
        val total = recent.sumOf { it.podThroughput.values.sum() + it.queueThroughput.values.sum() }
        return total.toDouble() / span
    }

    fun getKafkaLagData(): Map<String, Kafka.LagInfo> = synchronized(lock) { kafkaLagData.toMap() }

    fun setOnDataUpdateCallback(callback: () -> Unit) {
        // No-op; event-driven model updates synchronously on insert.
    }

    // Internals ----------------------------------------------------------------------------------

    private fun recordLogMessageAt(line: DomainLine, ts: Instant) = synchronized(lock) {
        // Throughput increments (pod or queue)
        val podIncrement: (MutableAggregatedThroughput) -> Unit = when (line) {
            is LogLineDomain -> { acc ->
                val pod = line.serviceName
                acc.totalPodThroughput[pod] = (acc.totalPodThroughput[pod] ?: 0L) + 1L
            }
            is KafkaLineDomain -> { acc ->
                val topic = line.topic
                acc.totalQueueThroughput[topic] = (acc.totalQueueThroughput[topic] ?: 0L) + 1L
            }
            else -> { _ -> } // Unknown line type: ignore throughput
        }

        // Log level increment
        val lvlInc: (MutableAggregatedLogLevel) -> Unit = { acc ->
            val lvl = line.level
            acc.totalCounts[lvl] = (acc.totalCounts[lvl] ?: 0L) + 1L
        }

        // Update all buckets and prune
        bucketSpecs.forEach { (minutes, spec) ->
            upsertThroughput(minutes, spec, ts, podIncrement)
            upsertLogLevel(minutes, spec, ts, lvlInc)
        }

        pruneAllBuckets(now = Clock.System.now())
    }

    private fun upsertThroughput(
        minutes: Int,
        spec: BucketSpec,
        ts: Instant,
        increment: (MutableAggregatedThroughput) -> Unit
    ) {
        val (start, end) = windowRange(ts, spec)
        val bucket = throughputBuckets.getValue(minutes)
        val agg = bucket.getOrPut(start) { MutableAggregatedThroughput(start = start, end = end) }
        increment(agg)
        agg.events += 1
    }

    private fun upsertLogLevel(
        minutes: Int,
        spec: BucketSpec,
        ts: Instant,
        increment: (MutableAggregatedLogLevel) -> Unit
    ) {
        val (start, end) = windowRange(ts, spec)
        val bucket = logLevelBuckets.getValue(minutes)
        val agg = bucket.getOrPut(start) { MutableAggregatedLogLevel(start = start, end = end) }
        increment(agg)
        agg.events += 1
    }

    private fun pruneAllBuckets(now: Instant) {
        bucketSpecs.forEach { (minutes, spec) ->
            val cutoff = now - spec.retention
            pruneBucket(throughputBuckets.getValue(minutes), cutoff)
            pruneBucket(logLevelBuckets.getValue(minutes), cutoff)
        }
    }

    private fun pruneBucket(bucket: LinkedHashMap<Instant, *>, cutoff: Instant) {
        // Time-based pruning
        val it = bucket.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            val start = entry.key
            val end = start + (when (val value = entry.value) {
                is MutableAggregatedThroughput -> (value.end - value.start)
                is MutableAggregatedLogLevel -> (value.end - value.start)
                else -> 0.seconds
            })
            if (end < cutoff) it.remove()
        }

        // Size bound (best-effort; remove oldest entries)
        while (bucket.size > maxBucketSize) {
            val oldestKey = bucket.keys.minByOrNull { it } ?: break
            bucket.remove(oldestKey)
        }
    }

    private fun recentPodsFromBuckets(minutes: Int): Set<String> {
        val tps = throughputBuckets[minutes]?.values ?: return emptySet()
        return tps.flatMap { it.totalPodThroughput.keys }.toSet()
    }

    private fun getAggregatedMetricsFromBucketsInternal(minutes: Int): Triple<Long, Long, Double> {
        val ll = getLogLevelData(minutes)
        val tp = getThroughputData(minutes)
        if (ll.isEmpty() || tp.isEmpty()) return Triple(0L, 0L, 0.0)

        val totalMessages = ll.sumOf { it.counts.values.sum() }
        val totalErrors = ll.sumOf { it.counts[LogLevel.ERROR] ?: 0L }

        val sorted = tp.sortedBy { it.messageTimestamp }
        val first = sorted.first().messageTimestamp
        val last = sorted.last().messageTimestamp
        val spanSeconds = max(1L, (last - first).inWholeSeconds)

        val totalThroughput = sorted.sumOf { it.podThroughput.values.sum() + it.queueThroughput.values.sum() }
        val avg = totalThroughput.toDouble() / spanSeconds

        return Triple(totalMessages, totalErrors, avg)
    }

    // Utils --------------------------------------------------------------------------------------

    private fun windowRange(ts: Instant, spec: BucketSpec): Pair<Instant, Instant> {
        val start = ts.alignToInterval(spec.windowSizeSeconds.toLong())
        val end = start + spec.windowSizeSeconds.seconds
        return start to end
    }

    private fun Instant.alignToInterval(intervalSeconds: Long): Instant {
        val aligned = epochSeconds - (epochSeconds % intervalSeconds)
        return Instant.fromEpochSeconds(aligned)
    }

    private fun Instant.roundDown(bucketSizeSeconds: Int): Instant {
        val aligned = epochSeconds - (epochSeconds % bucketSizeSeconds)
        return Instant.fromEpochSeconds(aligned)
    }
}