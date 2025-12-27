package util

import LogLevel
import app.KafkaLineDomain
import app.LogLineDomain
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class DrainCompressedDomainLineStoreCompressionTest {
    private fun baselineApprox(lines: List<LogLineDomain>): Long {
        var sum = 0L
        for (l in lines) {
            sum += l.message.toByteArray(Charsets.UTF_8).size
            sum += l.indexIdentifier.toByteArray(Charsets.UTF_8).size
            sum += l.threadName.toByteArray(Charsets.UTF_8).size
            sum += l.serviceName.toByteArray(Charsets.UTF_8).size
            sum += l.serviceVersion.toByteArray(Charsets.UTF_8).size
            sum += l.logger.toByteArray(Charsets.UTF_8).size
            sum += 8 + 8 + 4
        }
        return sum
    }

    data class SizeReport(
        val label: String,
        val n: Int,
        val baselineBytes: Long,
        val storedBytes: Long,
        val bytesPerLine: Double,
        val ratio: Double,
    )

    private fun printRatio(label: String, baselineBytes: Long, storedBytes: Long, n: Int) {
        val ratio = baselineBytes.toDouble() / storedBytes.toDouble()
        val bpl = storedBytes.toDouble() / n.toDouble()
        println(
            buildString {
                appendLine("=== $label ===")
                appendLine("N=$n")
                appendLine("Baseline bytes: $baselineBytes")
                appendLine("Stored bytes:   $storedBytes")
                appendLine("Ratio:          ${"%.2f".format(ratio)}x")
                appendLine("Bytes/line:     ${"%.2f".format(bpl)}")
            }
        )
    }

    private fun report(label: String, n: Int, baselineBytes: Long, storedBytes: Long): SizeReport {
        val bytesPerLine = storedBytes.toDouble() / n.toDouble()
        val ratio = baselineBytes.toDouble() / storedBytes.toDouble()
        val r = SizeReport(label, n, baselineBytes, storedBytes, bytesPerLine, ratio)

        println(
            buildString {
                appendLine("=== $label ===")
                appendLine("N=$n")
                appendLine("Baseline bytes: $baselineBytes")
                appendLine("Stored bytes:   $storedBytes")
                appendLine("Ratio:          ${"%.2f".format(ratio)}x (baseline / stored)")
                appendLine("Bytes/line:     ${"%.2f".format(bytesPerLine)}")
            }
        )
        return r
    }

    /**
     * A deliberately "simple" baseline:
     * - sum of UTF-8 bytes for message and a few other string fields
     * - plus a fixed overhead per line for primitives
     *
     * This is not “exact raw object size”, but it’s stable and useful for comparing scenarios.
     */
    private fun baselineApproxBytesForLogLines(lines: List<LogLineDomain>): Long {
        var sum = 0L
        for (l in lines) {
            sum += l.message.toByteArray(Charsets.UTF_8).size
            sum += l.indexIdentifier.toByteArray(Charsets.UTF_8).size
            sum += l.threadName.toByteArray(Charsets.UTF_8).size
            sum += l.serviceName.toByteArray(Charsets.UTF_8).size
            sum += l.serviceVersion.toByteArray(Charsets.UTF_8).size
            sum += l.logger.toByteArray(Charsets.UTF_8).size

            l.correlationId?.let { sum += it.toByteArray(Charsets.UTF_8).size }
            l.requestId?.let { sum += it.toByteArray(Charsets.UTF_8).size }
            l.errorMessage?.let { sum += it.toByteArray(Charsets.UTF_8).size }
            l.stacktrace?.let { sum += it.toByteArray(Charsets.UTF_8).size }

            // rough primitive overhead (seq, timestamp, enums, etc)
            sum += 8 + 8 + 4
        }
        return sum
    }

    private fun baselineApproxBytesForKafkaLines(lines: List<KafkaLineDomain>): Long {
        var sum = 0L
        for (l in lines) {
            sum += l.message.toByteArray(Charsets.UTF_8).size
            sum += l.indexIdentifier.toByteArray(Charsets.UTF_8).size
            sum += l.topic.toByteArray(Charsets.UTF_8).size
            sum += l.headers.toByteArray(Charsets.UTF_8).size
            l.key?.let { sum += it.toByteArray(Charsets.UTF_8).size }
            l.correlationId?.let { sum += it.toByteArray(Charsets.UTF_8).size }
            l.requestId?.let { sum += it.toByteArray(Charsets.UTF_8).size }
            sum += l.compositeEventId.toByteArray(Charsets.UTF_8).size

            // rough primitive overhead (seq, timestamp, offset, partition, enums)
            sum += 8 + 8 + 8 + 4 + 4
        }
        return sum
    }

    private fun storeAllAndMeasureBytes(
        store: DrainCompressedDomainLineStore,
        ids: LongArray,
    ): Long {
        var total = 0L
        for (id in ids) {
            val b = store.getBytesBlocking(id)
            assertNotNull(b)
            total += b.size.toLong()
        }
        return total
    }
    @Test
    fun `message roundtrip preserves variables before and after seal`() = runBlocking {
        val store = DrainCompressedDomainLineStore(this)

        val ids = (0 until 100).map { i ->
            store.put(
                LogLineDomain(
                    seq = i.toLong(),
                    level = LogLevel.INFO,
                    timestamp = 1_700_000_000_000L,
                    message = "GET /api/orders id=42 timeMs=${1000 + i}",
                    indexIdentifier = "pod-A",
                    threadName = "t",
                    serviceName = "svc",
                    serviceVersion = "1",
                    logger = "log",
                )
            )
        }

        // Invariant 1: before seal
        ids.forEachIndexed { i, id ->
            val msg = store.getLineBlocking(id)!!.message
            assertEquals(
                "GET /api/orders id=42 timeMs=${1000 + i}",
                msg,
                "pre-seal corruption at index $i"
            )
        }

        store.seal().await()

        // Invariant 2: after seal
        ids.forEachIndexed { i, id ->
            val msg = store.getLineBlocking(id)!!.message
            assertEquals(
                "GET /api/orders id=42 timeMs=${1000 + i}",
                msg,
                "post-seal corruption at index $i"
            )
        }
    }
    @Test
    fun `check log template production`(): Unit = runBlocking {
        val drainCompressedDomainLineStore = DrainCompressedDomainLineStore(this)

        val longs = mutableListOf<Long>()
        for (i in 0..1000){
            longs.add(
                drainCompressedDomainLineStore.put(
                    LogLineDomain(
                        seq = i.toLong(),
                        level = LogLevel.INFO,
                        timestamp = 1_700_000_000_000L,
                        message = "GET /api/orders id=1000000 status=200 timeMs=100${i}",
                        indexIdentifier = "pod-A",
                        threadName = "http-nio-8080-exec-1",
                        serviceName = "orders",
                        serviceVersion = "1.2.3",
                        logger = "com.example.OrderController",
                    )
                )
            )
        }

     //  println("line: " + drainCompressedDomainLineStore.getMessageTemplate(longs[0]))
        drainCompressedDomainLineStore.getLineBlocking(longs[0])?.let { println("message: " + it.message) }


        drainCompressedDomainLineStore.seal().await()
        //println("line: " +drainCompressedDomainLineStore.getMessageTemplateSealed(longs[0]))
        drainCompressedDomainLineStore.getLineBlocking(longs[0])?.let {  println("message: " + it.message)  }
        println("Done")
    }

    @Test
    fun `loglike tokenizer improves template reuse for URL query heavy messages`() = runBlocking {
        val n = 50_000
        val baseTs = 1_700_000_000_000L

        fun mkMessage(i: Int): String {
            val ms = (i % 900) + 1
            val code = if (i % 50 == 0) 500 else 200
            val id = 100_000_000 + (i % 10_000) // repeats to encourage clustering
            val pageSize = 3
            val pageOffset = i % 200
            return "#---GRAVTY-END--- ${ms}ms for $code OK GET /transactions/v2/$id?page_size=$pageSize&page_offset=$pageOffset"
        }

        val lines = ArrayList<LogLineDomain>(n)
        for (i in 0 until n) {
            lines += LogLineDomain(
                seq = i.toLong(),
                level = LogLevel.INFO,
                timestamp = baseTs + i,
                message = mkMessage(i),
                indexIdentifier = "pod-A",
                threadName = "http-nio-8080-exec-1",
                serviceName = "tx",
                serviceVersion = "1.0.0",
                logger = "access",
                correlationId = null,
                requestId = null,
                errorMessage = null,
                stacktrace = null,
            )
        }

        val baselineBytes = baselineApprox(lines)

        suspend fun runStore(): Long {
            val store = DrainCompressedDomainLineStore(
                scope = CoroutineScope(Dispatchers.Default),
            )
            val ids = LongArray(n)
            for (i in 0 until n) ids[i] = store.put(lines[i])

            var stored = 0L
            for (id in ids) {
                val b = store.getBytesBlocking(id)
                assertNotNull(b)
                stored += b.size.toLong()
            }

            // verify decode round-trip for a couple
            val decoded0 = store.getLineBlocking(ids[0])
            val decodedLast = store.getLineBlocking(ids[n - 1])
            assertNotNull(decoded0)
            assertNotNull(decodedLast)

            return stored
        }


        val storedLogLike = runStore()
        printRatio("Tokenizer=LogLike (split '/', '?', '&', '=', ':')", baselineBytes, storedLogLike, n)
    }

    @Test
    fun `compresses when message has repeated pattern with small variables (LogLine)`(): Unit = runBlocking {
        val n = 500_000
        val scope = CoroutineScope(Dispatchers.Default)
        val store = DrainCompressedDomainLineStore(scope = scope)

        val baseTs = 1_700_000_000_000L

        // One stable pattern. Variables are numbers & short tokens.
        val lines = ArrayList<LogLineDomain>(n)
        for (i in 0 until n) {
            val msg = "GET /api/orders id=${100_000 + i} status=200 timeMs=${(i % 50) + 1}"
            lines += LogLineDomain(
                seq = i.toLong(),
                level = LogLevel.INFO,
                timestamp = baseTs + i,
                message = msg,
                indexIdentifier = "pod-A",
                threadName = "http-nio-8080-exec-1",
                serviceName = "orders",
                serviceVersion = "1.2.3",
                logger = "com.example.OrderController",
                correlationId = null,
                requestId = null,
                errorMessage = null,
                stacktrace = null,
            )
        }

        val ids = LongArray(n)
        for (i in 0 until n) ids[i] = store.put(lines[i])
        println("Before: ${store.estimateMemoryUsage()}")
        store.seal().await()
        println("After: ${store.estimateMemoryUsage()}")

        val storedBytes = storeAllAndMeasureBytes(store, ids)
        val baselineBytes = baselineApproxBytesForLogLines(lines)

        report("LogLine: single stable template + numeric vars", n, baselineBytes, storedBytes)

    }

    @Test
    fun `seal triggers packed compaction and preserves reads`() = runBlocking {
        val n = 20_000
        val scope = CoroutineScope(Dispatchers.Default)
        val store = DrainCompressedDomainLineStore(scope = scope)

        val baseTs = 1_700_000_000_000L
        val ids = LongArray(n)

        for (i in 0 until n) {
            val msg = "GET /api/orders id=${100_000 + i} status=200 timeMs=${(i % 50) + 1}"
            val line = LogLineDomain(
                seq = i.toLong(),
                level = LogLevel.INFO,
                timestamp = baseTs + i,
                message = msg,
                indexIdentifier = "pod-A",
                threadName = "http-nio-8080-exec-1",
                serviceName = "orders",
                serviceVersion = "1.2.3",
                logger = "com.example.OrderController",
                correlationId = null,
                requestId = null,
                errorMessage = null,
                stacktrace = null,
            )
            ids[i] = store.put(line)
        }

        val beforeKind = store.storageKind()
        store.seal().await()
        assertTrue(store.isSealed())

        assertEquals(DrainCompressedDomainLineStore.StorageKind.PACKED, store.storageKind())
        assertTrue(store.packedByteSizeOrZero() > 0)

        val decoded0 = store.getLineBlocking(ids[0])
        val decodedLast = store.getLineBlocking(ids[n - 1])
        assertNotNull(decoded0)
        assertNotNull(decodedLast)
        assertEquals(ids[0], decoded0.seq)
        assertEquals(ids[n - 1], decodedLast.seq)

        assertTrue(beforeKind == DrainCompressedDomainLineStore.StorageKind.MAP)
    }

    @Test
    fun `does NOT compress well when every message is unique text (LogLine)`(): Unit = runBlocking {
        val n = 30_000
        val scope = CoroutineScope(Dispatchers.Default)
        val store = DrainCompressedDomainLineStore(scope = scope)

        val baseTs = 1_700_000_000_000L
        val rnd = Random(123)

        // Unique-ish messages: lots of unique tokens => Drain learns many templates, little reuse.
        val lines = ArrayList<LogLineDomain>(n)
        for (i in 0 until n) {
            val randomToken = buildString(24) {
                repeat(24) {
                    val c = 'a'.code + rnd.nextInt(26)
                    append(c.toChar())
                }
            }
            val msg = "event=$randomToken payload=${randomToken.reversed()} idx=$i"
            lines += LogLineDomain(
                seq = i.toLong(),
                level = LogLevel.INFO,
                timestamp = baseTs + i,
                message = msg,
                indexIdentifier = "pod-A",
                threadName = "t-${i % 32}",
                serviceName = "svc-${i % 5}",
                serviceVersion = "1.2.3",
                logger = "com.example.Logger",
                correlationId = null,
                requestId = null,
                errorMessage = null,
                stacktrace = null,
            )
        }

        val ids = LongArray(n)
        for (i in 0 until n) ids[i] = store.put(lines[i])

        val storedBytes = storeAllAndMeasureBytes(store, ids)
        val baselineBytes = baselineApproxBytesForLogLines(lines)

        report("LogLine: mostly unique messages (expected weak compression)", n, baselineBytes, storedBytes)
    }

    @Test
    fun `template text reuse suspicion - constant prefix but variable tokenization can still defeat reuse`(): Unit =
        runBlocking {
            val n = 50_000
            val scope = CoroutineScope(Dispatchers.Default)
            val store = DrainCompressedDomainLineStore(scope = scope)

            val baseTs = 1_700_000_000_000L

            // Two variants:
            // A) token boundaries stable => should cluster well
            // B) token boundaries unstable (e.g., embed punctuation) => can cause template churn
            val stable = ArrayList<LogLineDomain>(n)
            val unstable = ArrayList<LogLineDomain>(n)

            for (i in 0 until n) {
                val id = 1_000_000 + i
                val stableMsg = "userId $id action LOGIN result OK"
                val unstableMsg = "userId=$id action=LOGIN result=OK" // different tokenization (punctuation glued)

                stable += LogLineDomain(
                    seq = i.toLong(),
                    level = LogLevel.INFO,
                    timestamp = baseTs + i,
                    message = stableMsg,
                    indexIdentifier = "pod-A",
                    threadName = "auth-1",
                    serviceName = "auth",
                    serviceVersion = "9.9.9",
                    logger = "Auth",
                    correlationId = null,
                    requestId = null,
                    errorMessage = null,
                    stacktrace = null,
                )

                unstable += LogLineDomain(
                    seq = (i + n).toLong(),
                    level = LogLevel.INFO,
                    timestamp = baseTs + i + n,
                    message = unstableMsg,
                    indexIdentifier = "pod-A",
                    threadName = "auth-1",
                    serviceName = "auth",
                    serviceVersion = "9.9.9",
                    logger = "Auth",
                    correlationId = null,
                    requestId = null,
                    errorMessage = null,
                    stacktrace = null,
                )
            }

            // Store stable batch
            val stableIds = LongArray(n)
            for (i in 0 until n) stableIds[i] = store.put(stable[i])
            val stableStored = storeAllAndMeasureBytes(store, stableIds)
            val stableBaseline = baselineApproxBytesForLogLines(stable)
            report("Pattern reuse A: stable token boundaries", n, stableBaseline, stableStored)

            // Store unstable batch (same store to keep dictionary warm)
            val unstableIds = LongArray(n)
            for (i in 0 until n) unstableIds[i] = store.put(unstable[i])
            val unstableStored = storeAllAndMeasureBytes(store, unstableIds)
            val unstableBaseline = baselineApproxBytesForLogLines(unstable)
            report("Pattern reuse B: unstable token boundaries", n, unstableBaseline, unstableStored)
        }

    @Test
    fun `string dictionary reuse - repeated fields vs unique fields (KafkaLine)`(): Unit = runBlocking {
        val n = 40_000
        val scope = CoroutineScope(Dispatchers.Default)
        val store = DrainCompressedDomainLineStore(scope = scope)

        val baseTs = 1_700_000_000_000L

        // Same topic/headers/indexIdentifier reused heavily -> dictionary should help a lot.
        val reused = ArrayList<KafkaLineDomain>(n)
        for (i in 0 until n) {
            reused += KafkaLineDomain(
                seq = i.toLong(),
                level = LogLevel.KAFKA,
                timestamp = baseTs + i,
                message = "orderId ${1000 + (i % 500)} status OK", // small vars
                indexIdentifier = "orders-topic#0",
                topic = "orders-topic",
                key = null,
                offset = i.toLong(),
                partition = 0,
                headers = "X-App: orders | X-Env: prod",
                correlationId = null,
                requestId = null,
                compositeEventId = "orders-topic#0#$i"
            )
        }

        val reusedIds = LongArray(n)
        for (i in 0 until n) reusedIds[i] = store.put(reused[i])

        val reusedStored = storeAllAndMeasureBytes(store, reusedIds)
        val reusedBaseline = baselineApproxBytesForKafkaLines(reused)
        report("KafkaLine: repeated topic/headers/indexIdentifier", n, reusedBaseline, reusedStored)

        // Now: lots of unique string fields -> dictionary can't amortize as well.
        val unique = ArrayList<KafkaLineDomain>(n)
        for (i in 0 until n) {
            unique += KafkaLineDomain(
                seq = (i + n).toLong(),
                level = LogLevel.KAFKA,
                timestamp = baseTs + i + n,
                message = "orderId ${i} status OK",
                indexIdentifier = "topic-${i % 200}#${i % 10}",
                topic = "topic-${i % 200}",
                key = "key-$i",
                offset = i.toLong(),
                partition = i % 10,
                headers = "X-Req: $i | X-Rand: ${i * 17}",
                correlationId = "corr-$i",
                requestId = "req-$i",
                compositeEventId = "topic-${i % 200}#${i % 10}#$i"
            )
        }

        val uniqueIds = LongArray(n)
        for (i in 0 until n) uniqueIds[i] = store.put(unique[i])

        val uniqueStored = storeAllAndMeasureBytes(store, uniqueIds)
        val uniqueBaseline = baselineApproxBytesForKafkaLines(unique)
        report("KafkaLine: mostly unique strings (expected weaker dictionary gains)", n, uniqueBaseline, uniqueStored)
    }

    @Test
    fun `seal blocks puts but allows reads`() = runBlocking {
        val store = DrainCompressedDomainLineStore(scope = CoroutineScope(Dispatchers.Default))

        val baseTs = 1_700_000_000_000L
        val line0 = LogLineDomain(
            seq = 1L,
            level = LogLevel.INFO,
            timestamp = baseTs,
            message = "GET /v1/hello/user id=123 status=200",
            indexIdentifier = "pod-A",
            threadName = "t",
            serviceName = "svc",
            serviceVersion = "1.0.0",
            logger = "l",
            correlationId = null,
            requestId = null,
            errorMessage = null,
            stacktrace = null,
        )
        val id0 = store.put(line0)

        store.seal()

        assertFailsWith<IllegalStateException> {
            store.put(
                line0.copy(
                    seq = 2L,
                    timestamp = baseTs + 1,
                    message = "GET /v1/hello/user id=124 status=200"
                )
            )
        }

        val bytes = store.getBytesBlocking(id0)
        assertNotNull(bytes)

        val decoded = store.getLineBlocking(id0)
        assertNotNull(decoded)
        assertEquals(line0.seq, decoded.seq)
        assertEquals(line0.indexIdentifier, decoded.indexIdentifier)
        assertEquals(line0.level, decoded.level)
        assertEquals(line0.timestamp, decoded.timestamp)
        assertTrue(decoded.message.isNotBlank())
    }

    @Test
    fun `seal is idempotent`(): Unit = runBlocking {
        val store = DrainCompressedDomainLineStore(scope = CoroutineScope(Dispatchers.Default))

        val baseTs = 1_700_000_000_000L
        val line0 = LogLineDomain(
            seq = 10L,
            level = LogLevel.INFO,
            timestamp = baseTs,
            message = "hello world",
            indexIdentifier = "pod-A",
            threadName = "t",
            serviceName = "svc",
            serviceVersion = "1.0.0",
            logger = "l",
            correlationId = null,
            requestId = null,
            errorMessage = null,
            stacktrace = null,
        )
        val id0 = store.put(line0)

        store.seal()
        store.seal()
        store.seal()

        val decoded = store.getLineBlocking(id0)
        assertNotNull(decoded)
        assertEquals(line0.seq, decoded.seq)

        assertFailsWith<IllegalStateException> {
            store.put(
                line0.copy(
                    seq = 11L,
                    timestamp = baseTs + 1
                )
            )
        }
    }
}