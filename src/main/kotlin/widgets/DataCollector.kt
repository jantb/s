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
 * Event-driven, bucketed data collector optimized for very fast inserts.
 *
 * Key changes from the LinkedHashMap design:
 * - Fixed-size ring buffers per (retention, window) spec. O(1) insert.
 * - No pruning passes on insert. Retention is enforced by ring overwrites.
 * - Live counters are also bucketed (e.g., 5 min -> 10s windows).
 * - Aggregates reuse per-slot objects; their maps are allocated lazily.
 *
 * Windows:
 * - 5 minutes -> 10-second windows (fine-grained live view).
 * - 15, 60, 360, 1440 minutes -> 60-second windows.
 *
 * All calculations are done on insert; reads materialize immutable DTOs.
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

    // --------------------------------------------------------------------------------------------
    // Configuration
    // --------------------------------------------------------------------------------------------

    private data class BucketSpec(
        val retention: Duration,
        val window: Duration
    ) {
        val windowSeconds: Long = window.inWholeSeconds
        val retentionSeconds: Long = retention.inWholeSeconds
        val capacity: Int = ((retentionSeconds + windowSeconds - 1) / windowSeconds).toInt().coerceAtLeast(1)
    }

    // Keep the same intervals; change windows per doc comment
    private val intervals = listOf(5, 15, 60, 360, 1440)
    private val bucketSpecs: Map<Int, BucketSpec> = mapOf(
        5 to BucketSpec(5.minutes, 10.seconds),
        15 to BucketSpec(15.minutes, 60.seconds),
        60 to BucketSpec(60.minutes, 60.seconds),
        360 to BucketSpec(360.minutes, 60.seconds),
        1440 to BucketSpec(1440.minutes, 60.seconds)
    )

    // --------------------------------------------------------------------------------------------
    // Aggregates (internal, mutable, reused per ring slot)
    // --------------------------------------------------------------------------------------------

    private interface ResettableAggregate {
        fun reset()
        fun isEmpty(): Boolean
    }

    private class MutableAggregatedThroughput : ResettableAggregate {
        // Lazily allocate maps to minimize memory footprint
        var totalPodThroughput: MutableMap<String, Long>? = null
        var totalQueueThroughput: MutableMap<String, Long>? = null
        var events: Int = 0

        fun incPod(pod: String) {
            val map = totalPodThroughput ?: HashMap<String, Long>().also { totalPodThroughput = it }
            map[pod] = (map[pod] ?: 0L) + 1L
            events++
        }

        fun incQueue(topic: String) {
            val map = totalQueueThroughput ?: HashMap<String, Long>().also { totalQueueThroughput = it }
            map[topic] = (map[topic] ?: 0L) + 1L
            events++
        }

        override fun reset() {
            totalPodThroughput?.clear()
            totalQueueThroughput?.clear()
            events = 0
        }

        override fun isEmpty(): Boolean = events == 0
    }

    private class MutableAggregatedLogLevel : ResettableAggregate {
        var totalCounts: MutableMap<LogLevel, Long>? = null
        var events: Int = 0

        fun inc(level: LogLevel) {
            val map = totalCounts ?: HashMap<LogLevel, Long>().also { totalCounts = it }
            map[level] = (map[level] ?: 0L) + 1L
            events++
        }

        override fun reset() {
            totalCounts?.clear()
            events = 0
        }

        override fun isEmpty(): Boolean = events == 0
    }

    // --------------------------------------------------------------------------------------------
    // Ring buffer buckets
    // --------------------------------------------------------------------------------------------

    private class RingBucket<A : ResettableAggregate>(
        private val spec: BucketSpec,
        factory: () -> A
    ) {
        private val capacity = spec.capacity
        private val windowSeconds = spec.windowSeconds

        // Sentinel for "uninitialized"
        private val UNSET = Long.MIN_VALUE

        // Per-slot window start epoch seconds; used to detect window rollover
        private val starts = LongArray(capacity) { UNSET }
        private val aggregates: MutableList<A> = MutableList(capacity) { factory() }

        private fun alignToWindowStart(epochSeconds: Long): Long =
            epochSeconds - (epochSeconds % windowSeconds)

        private fun indexForStart(startEpochSeconds: Long): Int {
            val windowIndex = startEpochSeconds / windowSeconds
            var idx = (windowIndex % capacity).toInt()
            if (idx < 0) idx += capacity
            return idx
        }

        fun update(ts: Instant, updater: (A) -> Unit) {
            val startEpoch = alignToWindowStart(ts.epochSeconds)
            val idx = indexForStart(startEpoch)
            if (starts[idx] != startEpoch) {
                aggregates[idx].reset()
                starts[idx] = startEpoch
            }
            updater(aggregates[idx])
        }

        /**
         * Returns non-empty windows within retention, oldest -> newest.
         * Returned pairs are (windowEndInstant, aggregate).
         */
        fun collect(now: Instant): List<Pair<Instant, A>> {
            val cutoffEpoch = now.epochSeconds - spec.retentionSeconds
            val items = ArrayList<Triple<Long, Long, A>>(capacity)
            for (i in 0 until capacity) {
                val s = starts[i]
                if (s == UNSET) continue
                val e = s + windowSeconds
                if (e <= cutoffEpoch) continue
                val agg = aggregates[i]
                if (agg.isEmpty()) continue
                items += Triple(s, e, agg)
            }
            items.sortBy { it.second } // end time ascending
            return items.map { (_, end, agg) -> Instant.fromEpochSeconds(end) to agg }
        }

        fun recentKeys(now: Instant, selector: (A) -> Map<String, Long>?): Set<String> {
            val cutoffEpoch = now.epochSeconds - spec.retentionSeconds
            val acc = mutableSetOf<String>()
            for (i in 0 until capacity) {
                val s = starts[i]
                if (s == UNSET) continue
                val e = s + windowSeconds
                if (e <= cutoffEpoch) continue
                val map = selector(aggregates[i]) ?: emptyMap()
                if (map.isNotEmpty()) acc += map.keys
            }
            return acc
        }
    }

    // --------------------------------------------------------------------------------------------
    // State
    // --------------------------------------------------------------------------------------------

    private val throughputBuckets: Map<Int, RingBucket<MutableAggregatedThroughput>> =
        bucketSpecs.mapValues { RingBucket(it.value) { MutableAggregatedThroughput() } }

    private val logLevelBuckets: Map<Int, RingBucket<MutableAggregatedLogLevel>> =
        bucketSpecs.mapValues { RingBucket(it.value) { MutableAggregatedLogLevel() } }

    private val activePods = mutableSetOf<String>() // explicitly registered/known pods
    private val kafkaLagData = mutableMapOf<String, Kafka.LagInfo>()

    // Thread-safety
    private val lock = Any()

    // --------------------------------------------------------------------------------------------
    // Public immutable DTOs (unchanged)
    // --------------------------------------------------------------------------------------------

    data class ThroughputDataPoint(
        val messageTimestamp: Instant, // window end
        val podThroughput: Map<String, Long>,
        val queueThroughput: Map<String, Long>
    )

    data class LogLevelDataPoint(
        val messageTimestamp: Instant, // window end
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

    // --------------------------------------------------------------------------------------------
    // Public interface
    // --------------------------------------------------------------------------------------------

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
        val now = Clock.System.now()
        (recentPodsFromBuckets(now, 5) + activePods).toSet()
    }

    fun getActivePodsCount(): Int = getActivePods().size

    // --------------------------------------------------------------------------------------------
    // Data retrieval
    // --------------------------------------------------------------------------------------------

    fun getThroughputData(minutes: Int = 5): List<ThroughputDataPoint> = synchronized(lock) {
        val now = Clock.System.now()
        val ring = throughputBuckets[minutes] ?: return@synchronized emptyList()
        ring
            .collect(now)
            .map { (end, agg) ->
                ThroughputDataPoint(
                    messageTimestamp = end,
                    podThroughput = agg.totalPodThroughput?.toMap() ?: emptyMap(),
                    queueThroughput = agg.totalQueueThroughput?.toMap() ?: emptyMap()
                )
            }
    }

    fun getLogLevelData(minutes: Int = 5): List<LogLevelDataPoint> = synchronized(lock) {
        val now = Clock.System.now()
        val ring = logLevelBuckets[minutes] ?: return@synchronized emptyList()
        ring
            .collect(now)
            .map { (end, agg) ->
                LogLevelDataPoint(
                    messageTimestamp = end,
                    counts = agg.totalCounts?.toMap() ?: emptyMap()
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
        // 5-min bucket uses 10s windows; filter it
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

    // --------------------------------------------------------------------------------------------
    // Metrics (from buckets only)
    // --------------------------------------------------------------------------------------------

    fun getRawMetricsFromBuckets(): Triple<Long, Long, Double> =
        getAggregatedMetricsFromBucketsInternal(minutes = 5)

    fun getAggregatedMetricsFromBuckets(minutes: Int): Triple<Long, Long, Double> =
        getAggregatedMetricsFromBucketsInternal(minutes)

    fun getMetricsFromHistory(minutes: Int): Triple<Long, Long, Double> =
        getAggregatedMetricsFromBucketsInternal(minutes)

    // --------------------------------------------------------------------------------------------
    // Lifecycle
    // --------------------------------------------------------------------------------------------

    fun shutdown() {
        // No background jobs
    }

    // --------------------------------------------------------------------------------------------
    // DashboardService helpers (derived purely from buckets)
    // --------------------------------------------------------------------------------------------

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

    // --------------------------------------------------------------------------------------------
    // Internals
    // --------------------------------------------------------------------------------------------

    private fun recordLogMessageAt(line: DomainLine, ts: Instant) = synchronized(lock) {
        // Throughput increments (pod or queue)
        when (line) {
            is LogLineDomain -> {
                val pod = line.serviceName
                throughputBuckets.forEach { (_, ring) -> ring.update(ts) { it.incPod(pod) } }
            }
            is KafkaLineDomain -> {
                val topic = line.topic
                throughputBuckets.forEach { (_, ring) -> ring.update(ts) { it.incQueue(topic) } }
            }
            else -> Unit // Unknown line type: ignore throughput
        }

        // Log level increment
        val lvl = line.level
        logLevelBuckets.forEach { (_, ring) -> ring.update(ts) { it.inc(lvl) } }
    }

    private fun recentPodsFromBuckets(now: Instant, minutes: Int): Set<String> {
        val ring = throughputBuckets[minutes] ?: return emptySet()
        return ring.recentKeys(now) { it.totalPodThroughput }
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

    // --------------------------------------------------------------------------------------------
    // Utils
    // --------------------------------------------------------------------------------------------

    private fun Instant.roundDown(bucketSizeSeconds: Int): Instant {
        val aligned = epochSeconds - (epochSeconds % bucketSizeSeconds)
        return Instant.fromEpochSeconds(aligned)
    }
}