package util

import LogLevel
import app.DomainLine
import app.KafkaLineDomain
import app.LogLineDomain
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min

class DrainCompressedDomainLineStore(
    private val scope: CoroutineScope,
    private val maxDepth: Int = 4,
    private val maxClustersPerLeaf: Int = 64,
    private val maxChildrenPerNode: Int = 128,
) {
    private interface Storage {
        fun getBytes(id: Long): ByteArray?
        fun putBytes(id: Long, bytes: ByteArray)
        fun snapshotEntries(): Array<Pair<Long, ByteArray>>
        fun size(): Int
        fun sampleEntries(max: Int): Array<Pair<Long, ByteArray>>
    }

    private class MapStorage(
        private val map: ConcurrentHashMap<Long, ByteArray>,
    ) : Storage {
        override fun getBytes(id: Long): ByteArray? = map[id]
        override fun putBytes(id: Long, bytes: ByteArray) {
            map[id] = bytes
        }

        override fun snapshotEntries(): Array<Pair<Long, ByteArray>> {
            val out = ArrayList<Pair<Long, ByteArray>>(map.size)
            for (e in map.entries) out.add(e.key to e.value)
            return out.toTypedArray()
        }

        override fun size(): Int = map.size
        override fun sampleEntries(max: Int): Array<Pair<Long, ByteArray>> {
            val out = ArrayList<Pair<Long, ByteArray>>(minOf(max, map.size))
            val it = map.entries.iterator()
            while (it.hasNext() && out.size < max) {
                val e = it.next()
                out.add(e.key to e.value)
            }
            return out.toTypedArray()
        }
    }

    private class PackedStorage(
        val header: SegmentHeader,
        private val baseId: Long,
        private val checkpointStride: Int,
        private val checkpointIds: LongArray,
        private val checkpointDeltaOffsets: IntArray,
        private val deltaBytes: ByteArray,
        private val offsets: IntArray,
        private val blob: ByteArray,
    ) : Storage {
        data class SegmentHeader(
            val domainType: DomainType,
            val level: LogLevel,
            val indexIdentifierId: Int
        )

        data class ByteSlice(val buf: ByteArray, val start: Int, val end: Int)

        fun packedByteSize(): Int = blob.size

        private data class VarIntRead(val value: Long, val nextPos: Int)

        private fun readUVarLong(buf: ByteArray, start: Int): VarIntRead {
            var x = 0L
            var s = 0
            var p = start
            while (true) {
                val b = buf[p++].toInt() and 0xff
                x = x or (((b and 0x7f).toLong()) shl s)
                if ((b and 0x80) == 0) return VarIntRead(x, p)
                s += 7
            }
        }

        private fun findIndex(id: Long): Int {
            if (checkpointIds.isEmpty()) return -1

            var lo = 0
            var hi = checkpointIds.size - 1
            while (lo <= hi) {
                val mid = (lo + hi) ushr 1
                val v = checkpointIds[mid]
                if (v < id) lo = mid + 1 else if (v > id) hi = mid - 1 else return mid * checkpointStride
            }

            val cp = hi
            if (cp < 0) return -1

            var curId = checkpointIds[cp]
            var idx = cp * checkpointStride
            var p = checkpointDeltaOffsets[cp]

            if (curId == id) return idx

            val maxIdx = offsets.size - 2
            val endIdx = min(maxIdx, (cp + 1) * checkpointStride + checkpointStride)

            while (idx < endIdx && idx < maxIdx) {
                val d = readUVarLong(deltaBytes, p)
                p = d.nextPos
                curId += d.value
                idx++
                if (curId == id) return idx
                if (curId > id) return -1
            }
            return -1
        }

        fun getByteSlice(id: Long): ByteSlice? {
            val idx = findIndex(id)
            if (idx < 0) return null
            val start = offsets[idx]
            val end = offsets[idx + 1]
            return ByteSlice(blob, start, end)
        }

        override fun getBytes(id: Long): ByteArray? {
            val s = getByteSlice(id) ?: return null
            val len = s.end - s.start
            val out = ByteArray(len)
            s.buf.copyInto(out, 0, s.start, s.end)
            return out
        }

        override fun putBytes(id: Long, bytes: ByteArray) {
            throw IllegalStateException("sealed")
        }

        override fun snapshotEntries(): Array<Pair<Long, ByteArray>> {
            val n = offsets.size - 1
            if (n <= 0) return emptyArray()
            val out = ArrayList<Pair<Long, ByteArray>>(n)

            var id = baseId
            out.add(id to blob.copyOfRange(offsets[0], offsets[1]))

            var idx = 0
            var p = 0
            while (idx < n - 1) {
                val d = readUVarLong(deltaBytes, p)
                p = d.nextPos
                id += d.value
                idx++
                out.add(id to blob.copyOfRange(offsets[idx], offsets[idx + 1]))
            }
            return out.toTypedArray()
        }

        override fun size(): Int = offsets.size - 1

        override fun sampleEntries(max: Int): Array<Pair<Long, ByteArray>> {
            val n = minOf(max, offsets.size - 1)
            if (n <= 0) return emptyArray()
            val out = ArrayList<Pair<Long, ByteArray>>(n)

            var id = baseId
            out.add(id to blob.copyOfRange(offsets[0], offsets[1]))

            var idx = 0
            var p = 0
            while (idx < n - 1) {
                val d = readUVarLong(deltaBytes, p)
                p = d.nextPos
                id += d.value
                idx++
                out.add(id to blob.copyOfRange(offsets[idx], offsets[idx + 1]))
            }
            return out.toTypedArray()
        }
    }

    private class VarIntByteWriter(initialCapacity: Int) {
        private var buf = ByteArray(maxOf(16, initialCapacity))
        private var pos = 0
        fun size(): Int = pos
        fun writeUVarLong(value: Long) {
            var v = value
            while ((v and 0x7fL.inv()) != 0L) {
                writeByte(((v and 0x7fL) or 0x80L).toInt())
                v = v ushr 7
            }
            writeByte(v.toInt())
        }

        private fun writeByte(b: Int) {
            if (pos == buf.size) buf = buf.copyOf(buf.size * 2)
            buf[pos++] = b.toByte()
        }

        fun toByteArray(): ByteArray = buf.copyOf(pos)
    }

    private val storageRef = AtomicReference<Storage>(
        MapStorage(ConcurrentHashMap<Long, ByteArray>(1 shl 16))
    )

    private val sealed = AtomicBoolean(false)
    private val compactionStarted = AtomicBoolean(false)

    private val baseTimestamp = AtomicLong(0L)

    private val templatesById = ConcurrentHashMap<Int, Array<String>>(4096)
    private var miner: DrainMiner? = DrainMiner(
        maxDepth = maxDepth,
        maxClustersPerLeaf = maxClustersPerLeaf,
        maxChildrenPerNode = maxChildrenPerNode,
    )
    private var writeCh: Channel<PutReq>? = Channel<PutReq>(capacity = Channel.BUFFERED)
    private val stringDict = StringDictionary(initialCapacity = 1 shl 14)
    private val uuidDict = UuidDictionary(initialCapacity = 1 shl 14)
    private var writerJob: Job?
    private val acceptWrites = AtomicBoolean(true)
    @Volatile private var useFrozenDecode = false
    init {
        writerJob = scope.launch(Dispatchers.Default + CoroutineName("DrainCompressedDomainLineStore-writer")) {
            for (req in writeCh!!) {
                if (!acceptWrites.get()) {
                    req.result.completeExceptionally(IllegalStateException("sealed"))
                    continue
                }
                val id = req.line.seq
                val bts = baseTimestamp.get()
                if (bts == 0L) baseTimestamp.compareAndSet(0L, req.line.timestamp)
                val bytes = encodeToBytes(line = req.line)
                storageRef.get().putBytes(id, bytes)
                req.result.complete(id)
            }
        }
    }

    private fun buildSegmentedTemplate(tokens: Array<String>): SegmentedTemplate {
        val segments = ArrayList<String>()
        val sb = StringBuilder()
        var estLen = 0

        for (t in tokens) {
            if (t === DrainMiner.VAR) {
                segments.add(sb.toString())
                sb.setLength(0)
            } else {
                sb.append(t)
                estLen += t.length
            }
        }
        segments.add(sb.toString())

        return SegmentedTemplate(
            segments = segments.toTypedArray(),
            estimatedLength = estLen
        )
    }

    @Volatile
    private var frozenTemplates: Array<SegmentedTemplate?>? = null
    private fun encodeToBytesFrozen(
        line: DomainLine,
        templateId: Int,
        varValues: List<String>
    ): ByteArray {
        val out = FastByteWriter(initialCapacity = 64 + line.message.length / 2)

        val bts = baseTimestamp.get()
        val relTs = if (bts == 0L) line.timestamp else line.timestamp - bts
        out.writeUVarLong(relTs)

        out.writeUVarInt(templateId)
        out.writeUVarInt(varValues.size)

        for (i in 0 until varValues.size) {
            out.writeUVarInt(1)
        }

        when (line) {
            is LogLineDomain -> {
                out.writeUVarInt(1)
                out.writeUVarInt(ExtraType.LOG.id)
                out.writeUVarInt(stringDict.idOf(line.threadName))
                out.writeUVarInt(stringDict.idOf(line.serviceName))
                out.writeUVarInt(stringDict.idOf(line.serviceVersion))
                out.writeUVarInt(stringDict.idOf(line.logger))
                out.writeNullableStringOrUuidId(line.correlationId)
                out.writeNullableStringOrUuidId(line.requestId)
                out.writeNullableStringOrUuidId(line.errorMessage)
                out.writeNullableStringOrUuidId(line.stacktrace)
            }

            is KafkaLineDomain -> {
                out.writeUVarInt(1)
                out.writeUVarInt(ExtraType.KAFKA.id)
                out.writeUVarInt(stringDict.idOf(line.topic))
                out.writeNullableStringId(stringDict, line.key)
                out.writeZigZagVarLong(line.offset)
                out.writeZigZagVarLong(line.partition.toLong())
                out.writeUVarInt(stringDict.idOf(line.headers))
                out.writeNullableStringOrUuidId(line.correlationId)
                out.writeNullableStringOrUuidId(line.requestId)
                out.writeUVarInt(stringDict.idOf(line.compositeEventId))
            }
        }

        for (v in varValues) encodeVariable(out, v)

        return out.toByteArray()
    }

    fun seal(): CountDownLatch {
        val cdl = CountDownLatch(1)
        if (!acceptWrites.compareAndSet(true, false)) {
            cdl.countDown()
            return cdl
        }
        writeCh!!.close()
        if (!sealed.compareAndSet(false, true)) {
            cdl.countDown()
            return cdl
        }
        if (!compactionStarted.compareAndSet(false, true)) {
            cdl.countDown()
            return cdl
        }

        scope.launch(Dispatchers.Default + CoroutineName("DrainCompressedDomainLineStore-seal")) {
            try {
                writerJob!!.join()

                val before = storageRef.get()
                val entries = before.snapshotEntries().sortedBy { it.first }

                if (entries.isEmpty()) {
                    storageRef.set(
                        PackedStorage(
                            baseId = 0L,
                            checkpointStride = 128,
                            checkpointIds = LongArray(0),
                            checkpointDeltaOffsets = IntArray(0),
                            deltaBytes = ByteArray(0),
                            offsets = IntArray(1),
                            blob = ByteArray(0),
                            header = PackedStorage.SegmentHeader(
                                domainType = DomainType.LOG,
                                level = LogLevel.INFO,
                                indexIdentifierId = stringDict.idOf("default")
                            )
                        )
                    )
                    frozenTemplates = emptyArray()
                    templatesById.clear()
                    return@launch
                }

                val decoded = ArrayList<DomainLine>(entries.size)
                val decodedIds = ArrayList<Long>(entries.size)

                for ((id, bytes) in entries) {
                    val line = decodeFull(bytes, bytes.size, id)
                    if (line != null) {
                        decoded.add(line)
                        decodedIds.add(id)
                    }
                }

                if (decoded.isEmpty()) {
                    storageRef.set(
                        PackedStorage(
                            baseId = 0L,
                            checkpointStride = 128,
                            checkpointIds = LongArray(0),
                            checkpointDeltaOffsets = IntArray(0),
                            deltaBytes = ByteArray(0),
                            offsets = IntArray(1),
                            blob = ByteArray(0),
                            header = PackedStorage.SegmentHeader(
                                domainType = DomainType.LOG,
                                level = LogLevel.INFO,
                                indexIdentifierId = stringDict.idOf("default")
                            )
                        )
                    )
                    frozenTemplates = emptyArray()
                    templatesById.clear()
                    return@launch
                }

                val miner = DrainMiner(maxDepth, maxClustersPerLeaf, maxChildrenPerNode)

                val tokensByLine = ArrayList<Array<String>>(decoded.size)
                val templateIdsByLine = ArrayList<Int>(decoded.size)
                val templateTokensByLine = ArrayList<Array<String>>(decoded.size)

                for (line in decoded) {
                    val tokens = MessageTokenizer.tokenize(line.message, MessageTokenizer.Mode.LogLike)
                    val cluster = miner.learn(tokens)

                    val templateSnapshotTokens = cluster.templateTokens.copyOf()

                    tokensByLine.add(tokens)
                    templateIdsByLine.add(cluster.id)
                    templateTokensByLine.add(templateSnapshotTokens)
                }

                val finalTemplateTokensById = HashMap<Int, Array<String>>()
                for (i in decoded.indices) {
                    finalTemplateTokensById[templateIdsByLine[i]] = templateTokensByLine[i]
                }

                val maxTid = finalTemplateTokensById.keys.maxOrNull() ?: 0
                val frozenSeg = arrayOfNulls<SegmentedTemplate>(maxTid + 1)
                val frozenTok = arrayOfNulls<Array<String>>(maxTid + 1)

                for ((tid, toks) in finalTemplateTokensById) {
                    frozenTok[tid] = toks
                    frozenSeg[tid] = buildSegmentedTemplate(toks)
                }

                val WILDCARD = "<*>"
                val varValuesByLine = ArrayList<List<String>>(decoded.size)

                for (i in decoded.indices) {
                    val lineTokens = tokensByLine[i]
                    val tid = templateIdsByLine[i]
                    val tmplTokens = frozenTok[tid]
                        ?: error("Missing frozen template tokens for tid=$tid")

                    val vars = ArrayList<String>()
                    val m = minOf(lineTokens.size, tmplTokens.size)
                    for (j in 0 until m) {
                        if (tmplTokens[j] == WILDCARD) {
                            vars.add(lineTokens[j])
                        }
                    }
                    varValuesByLine.add(vars)
                }

                val rewritten = ArrayList<Pair<Long, ByteArray>>(decoded.size)

                for (i in decoded.indices) {
                    val line = decoded[i]
                    val tid = templateIdsByLine[i]
                    val bytes = encodeToBytesFrozen(
                        line = line,
                        templateId = tid,
                        varValues = varValuesByLine[i]
                    )

                    rewritten.add(decodedIds[i] to bytes)
                }

                rewritten.sortBy { it.first }

                val n = rewritten.size
                val checkpointStride = 128
                val checkpointCount = (n + checkpointStride - 1) / checkpointStride

                val checkpointIds = LongArray(checkpointCount)
                val checkpointDeltaOffsets = IntArray(checkpointCount)

                val offsets = IntArray(n + 1)
                var total = 0
                for (i in 0 until n) {
                    offsets[i] = total
                    total += rewritten[i].second.size
                }
                offsets[n] = total

                val blob = ByteArray(total)
                var pos = 0
                for ((_, b) in rewritten) {
                    b.copyInto(blob, pos)
                    pos += b.size
                }

                val deltaWriter = VarIntByteWriter(n * 2)
                for (i in 0 until n) {
                    if (i % checkpointStride == 0) {
                        val cp = i / checkpointStride
                        checkpointIds[cp] = rewritten[i].first
                        checkpointDeltaOffsets[cp] = deltaWriter.size()
                    }
                    if (i > 0) {
                        deltaWriter.writeUVarLong(rewritten[i].first - rewritten[i - 1].first)
                    }
                }
                frozenTemplates = frozenSeg

                storageRef.set(
                    PackedStorage(
                        baseId = rewritten[0].first,
                        checkpointStride = checkpointStride,
                        checkpointIds = checkpointIds,
                        checkpointDeltaOffsets = checkpointDeltaOffsets,
                        deltaBytes = deltaWriter.toByteArray(),
                        offsets = offsets,
                        blob = blob,
                        header = PackedStorage.SegmentHeader(
                            domainType = DomainType.of(decoded.first()),
                            level = decoded.first().level,
                            indexIdentifierId = stringDict.idOf(decoded.first().indexIdentifier)
                        )
                    )
                )
                useFrozenDecode = true
                templatesById.clear()
                uuidDict.freeze()
                stringDict.freeze()
            } finally {
                miner = null
                writeCh = null
                writerJob = null
                cdl.countDown()
            }
        }

        return cdl
    }

    class UuidDictionary(initialCapacity: Int = 16) {
        private val lock = Any()
        private val toId = HashMap<String, Int>(initialCapacity)
        val fromId = ArrayList<ByteArray>(initialCapacity) // store as 16-byte UUID

        fun idOf(s: String): Int {
            val existing = toId[s]
            if (existing != null) return existing
            synchronized(lock) {
                val existing2 = toId[s]
                if (existing2 != null) return existing2
                val id = fromId.size
                fromId.add(uuidToBytes(s))
                toId[s] = id
                return id
            }
        }

        private fun uuidToBytes(s: String): ByteArray {
            val b = ByteArray(16)
            var bi = 0
            var i = 0
            while (i < s.length) {
                if (s[i] == '-') {
                    i++; continue
                }
                val hi = hexVal(s[i])
                val lo = hexVal(s[i + 1])
                b[bi++] = ((hi shl 4) or lo).toByte()
                i += 2
            }
            return b
        }

        private fun hexVal(c: Char): Int = when (c) {
            in '0'..'9' -> c - '0'
            in 'a'..'f' -> c - 'a' + 10
            in 'A'..'F' -> c - 'A' + 10
            else -> 0
        }

        private lateinit var frozenFromId: Array<ByteArray>

        fun freeze() {
            synchronized(lock) {
                frozenFromId = fromId.toTypedArray()
                toId.clear()
                fromId.clear()
                fromId.trimToSize()
            }
        }

        fun get(id: Int): String {
            val bytes =  if (::frozenFromId.isInitialized) {
                 frozenFromId[id]
            } else {
                fromId[id]
            }

            return buildString(36) {
                for (i in 0 until 16) {
                    val v = bytes[i].toInt() and 0xff
                    append(HEX[v ushr 4])
                    append(HEX[v and 0x0f])
                    when (i) {
                        3, 5, 7, 9 -> append('-')
                    }
                }
            }
        }

        companion object {
            private val HEX = "0123456789abcdef".toCharArray()
        }
    }

    suspend fun put(line: DomainLine): Long {
        if (!acceptWrites.get()) throw IllegalStateException("sealed")
        val d = CompletableDeferred<Long>()
        writeCh!!.send(PutReq(line, d))
        return d.await()
    }

    fun getBytesBlocking(id: Long): ByteArray? = storageRef.get().getBytes(id)

    fun getLineBlocking(id: Long): DomainLine? {
        val s = storageRef.get()
        return when (s) {
            is PackedStorage -> {
                val slice = s.getByteSlice(id) ?: return null
                decodeFullSealed(s, slice.buf, slice.start, slice.end, id)
            }

            else -> {
                val bytes = s.getBytes(id) ?: return null
                decodeFullMutable(bytes, bytes.size, id)
            }
        }
    }


    private fun encodeToBytes(line: DomainLine): ByteArray {
        val domainType = DomainType.of(line)
        val msgTokens = MessageTokenizer.tokenize(line.message, MessageTokenizer.Mode.LogLike)
        val cluster = miner!!.learn(msgTokens)
        val templateId = cluster.id
        templatesById.putIfAbsent(templateId, cluster.templateTokens.copyOf())
        val tmpl = cluster.templateTokens
        val tokenCount = msgTokens.size
        val varPositions = IntArray(tokenCount)
        val vars = ArrayList<String>(8)
        var varCount = 0
        for (i in 0 until tokenCount) {
            if (tmpl[i] === DrainMiner.VAR) {
                varPositions[varCount++] = i
                vars.add(msgTokens[i])
            }
        }
        val indexIdentifierId = stringDict.idOf(line.indexIdentifier)
        val out = FastByteWriter(initialCapacity = 72 + (line.message.length ushr 2))
        out.writeUVarInt(domainType.id)
        val bts = baseTimestamp.get()
        val relTs = if (bts == 0L) line.timestamp else (line.timestamp - bts)
        out.writeUVarLong(relTs)
        out.writeUVarInt(templateId)
        out.writeUVarInt(varCount)
        var prev = 0
        for (k in 0 until varCount) {
            val p = varPositions[k]
            out.writeUVarInt(p - prev)
            prev = p
        }
        out.writeUVarInt(line.level.ordinal)
        out.writeUVarInt(indexIdentifierId)
        when (line) {
            is LogLineDomain -> {
                out.writeUVarInt(1)
                out.writeUVarInt(ExtraType.LOG.id)
                out.writeUVarInt(stringDict.idOf(line.threadName))
                out.writeUVarInt(stringDict.idOf(line.serviceName))
                out.writeUVarInt(stringDict.idOf(line.serviceVersion))
                out.writeUVarInt(stringDict.idOf(line.logger))
                out.writeNullableStringOrUuidId( line.correlationId)
                out.writeNullableStringOrUuidId( line.requestId)
                out.writeNullableStringOrUuidId( line.errorMessage)
                out.writeNullableStringOrUuidId( line.stacktrace)
            }

            is KafkaLineDomain -> {
                out.writeUVarInt(1)
                out.writeUVarInt(ExtraType.KAFKA.id)
                out.writeUVarInt(stringDict.idOf(line.topic))
                out.writeNullableStringId(stringDict, line.key)
                out.writeZigZagVarLong(line.offset)
                out.writeZigZagVarLong(line.partition.toLong())
                out.writeUVarInt(stringDict.idOf(line.headers))
                out.writeNullableStringOrUuidId( line.correlationId)
                out.writeNullableStringOrUuidId( line.requestId)
                out.writeUVarInt(stringDict.idOf(line.compositeEventId))
            }
        }
        for (v in vars) encodeVariable(out, v)
        return out.toByteArray()
    }

    private class SegmentedTemplate(
        val segments: Array<String>,
        val estimatedLength: Int
    )

    private data class DecodedPrefix(
        val domainType: DomainType,
        val timestamp: Long,
        val templateId: Int,
        val varPositions: IntArray,
        val inp: FastByteReader
    )

    private data class DecodedPrefixSealed(
        val timestamp: Long,
        val templateId: Int,
        val varPositions: IntArray,
        val inp: FastByteReader
    )

    private fun decodePrefix(
        bytes: ByteArray,
        start: Int,
        end: Int
    ): DecodedPrefix {
        val inp = FastByteReader(bytes, start, end)

        val domainType = DomainType.fromId(inp.readUVarInt())
        val relTs = inp.readUVarLong()
        val bts = baseTimestamp.get()
        val timestamp = if (bts == 0L) relTs else bts + relTs

        val templateId = inp.readUVarInt()
        val varCount = inp.readUVarInt()

        val positions = IntArray(varCount)
        var prev = 0
        for (i in 0 until varCount) {
            prev += inp.readUVarInt()
            positions[i] = prev
        }

        return DecodedPrefix(domainType, timestamp, templateId, positions, inp)
    }

    private fun decodePrefixSealed(
        bytes: ByteArray,
        start: Int,
        end: Int
    ): DecodedPrefixSealed {
        val inp = FastByteReader(bytes, start, end)

        val relTs = inp.readUVarLong()
        val bts = baseTimestamp.get()
        val timestamp = if (bts == 0L) relTs else bts + relTs

        val templateId = inp.readUVarInt()
        val varCount = inp.readUVarInt()

        val positions = IntArray(varCount)
        var prev = 0
        for (i in 0 until varCount) {
            prev += inp.readUVarInt()
            positions[i] = prev
        }

        return DecodedPrefixSealed(timestamp, templateId, positions, inp)
    }

    private fun decodeFullMutable(
        bytes: ByteArray,
        end: Int,
        id: Long
    ): DomainLine {
        val (domainType, timestamp, templateId, positions, inp) =
            decodePrefix(bytes, 0, end)

        val levelOrd = inp.readUVarInt()
        val indexIdentifier = stringDict.get(inp.readUVarInt())
        val extra = decodeExtra(inp)
        val level = domainType.levelFromOrdinal(levelOrd)

        val message = decodeMessage(templateId, positions, inp)

        return domainType.build(
            seq = id,
            level = level,
            timestamp = timestamp,
            message = message,
            indexIdentifier = indexIdentifier,
            extra = extra
        )
    }

    private fun decodeFullSealed(
        packed: PackedStorage,
        bytes: ByteArray,
        start: Int,
        end: Int,
        id: Long
    ): DomainLine {
        val (timestamp, templateId, positions, inp) =
            decodePrefixSealed(bytes, start, end)

        val header = packed.header
        val level = header.level
        val indexIdentifier = stringDict.get(header.indexIdentifierId)

        val extra = decodeExtra(inp)
        val message = decodeMessage(templateId, positions, inp)

        return header.domainType.build(
            seq = id,
            level = level,
            timestamp = timestamp,
            message = message,
            indexIdentifier = indexIdentifier,
            extra = extra
        )
    }

    private fun decodeMessage(templateId: Int, positions: IntArray, inp: FastByteReader): String {
        return if (useFrozenDecode && frozenTemplates != null) {
            val tmpl = frozenTemplates!![templateId]!!
            val segs = tmpl.segments
            val sb = StringBuilder(tmpl.estimatedLength)
            sb.append(segs[0])
            for (i in 1 until segs.size) {
                sb.append(decodeVariable(inp))
                sb.append(segs[i])
            }
            sb.toString()
        } else {
            val templateTokens = templatesById[templateId]
                ?: error("Missing templateId=$templateId")
            val tokens = templateTokens.copyOf()
            for (pos in positions) {
                val value = decodeVariable(inp)
                if (pos in tokens.indices) tokens[pos] = value
            }
            MessageTokenizer.join(tokens)
        }
    }

    private fun decodeFull(bytes: ByteArray, end: Int, id: Long): DomainLine? {
        val inp = FastByteReader(bytes, 0, end)
        val domainType = DomainType.fromId(inp.readUVarInt())
        val relTs = inp.readUVarLong()
        val bts = baseTimestamp.get()
        val timestamp = if (bts == 0L) relTs else bts + relTs

        val templateId = inp.readUVarInt()
        val varCount = inp.readUVarInt()

        val positions = IntArray(varCount)
        var prev = 0
        for (i in 0 until varCount) {
            prev += inp.readUVarInt()
            positions[i] = prev
        }

        val levelOrd = inp.readUVarInt()
        val indexIdentifier = stringDict.get(inp.readUVarInt())
        val extra = decodeExtra(inp)
        val level = domainType.levelFromOrdinal(levelOrd)

        val message = if (sealed.get() && frozenTemplates != null) {
            val tmpl = frozenTemplates!![templateId]!!
            val segs = tmpl.segments

            val sb = StringBuilder(tmpl.estimatedLength)
            sb.append(segs[0])

            for (i in 1 until segs.size) {
                sb.append(decodeVariable(inp))
                sb.append(segs[i])
            }

            sb.toString()
        } else {
            val templateTokens = templatesById[templateId] ?: return null
            val tokens = templateTokens.copyOf()

            for (i in 0 until varCount) {
                val pos = positions[i]
                val value = decodeVariable(inp)
                if (pos in tokens.indices) tokens[pos] = value
            }

            MessageTokenizer.join(tokens)
        }

        return domainType.build(
            seq = id,
            level = level,
            timestamp = timestamp,
            message = message,
            indexIdentifier = indexIdentifier,
            extra = extra
        )
    }

    private fun decodeExtra(inp: FastByteReader): Any? {
        if (inp.readUVarInt() != 1) return null
        return when (ExtraType.fromId(inp.readUVarInt())) {
            ExtraType.LOG -> DomainExtra.LogExtra(
                threadName = stringDict.get(inp.readUVarInt()),
                serviceName = stringDict.get(inp.readUVarInt()),
                serviceVersion = stringDict.get(inp.readUVarInt()),
                logger = stringDict.get(inp.readUVarInt()),
                correlationId = inp.readNullableStringOrUuidId(),
                requestId = inp.readNullableStringOrUuidId(),
                errorMessage = inp.readNullableStringOrUuidId(),
                stacktrace = inp.readNullableStringOrUuidId(),
            )

            ExtraType.KAFKA -> DomainExtra.KafkaExtra(
                topic = stringDict.get(inp.readUVarInt()),
                key = inp.readNullableStringId(stringDict),
                offset = inp.readZigZagVarLong(),
                partition = inp.readZigZagVarLong().toInt(),
                headers = stringDict.get(inp.readUVarInt()),
                correlationId = inp.readNullableStringOrUuidId(),
                requestId = inp.readNullableStringOrUuidId(),
                compositeEventId = stringDict.get(inp.readUVarInt()),
            )
        }
    }

    private fun encodeVariable(out: FastByteWriter, raw: String) {
        raw.toLongOrNull()?.let {
            out.writeUVarInt(VarTag.LONG.id)
            out.writeZigZagVarLong(it)
            return
        }
        val u = normalizeUuidCandidate(raw)
        if (u != null) {
            out.writeUVarInt(VarTag.UUID.id)
            out.writeUVarInt(uuidDict.idOf(u))
            return
        }
        out.writeUVarInt(VarTag.STRING.id)
        out.writeUtf8(raw)
    }

    private fun decodeVariable(inp: FastByteReader): String {
        return when (VarTag.fromId(inp.readUVarInt())) {
            VarTag.LONG -> inp.readZigZagVarLong().toString()
            VarTag.UUID -> {
                uuidDict.get(inp.readUVarInt())
            }

            VarTag.STRING -> inp.readUtf8()
        }
    }

    private data class PutReq(val line: DomainLine, val result: CompletableDeferred<Long>)

    private enum class VarTag(val id: Int) {
        LONG(1),
        UUID(2),
        STRING(3);

        companion object {
            fun fromId(id: Int): VarTag = when (id) {
                1 -> LONG
                2 -> UUID
                3 -> STRING
                else -> error("Unknown VarTag id=$id")
            }
        }
    }

    private enum class ExtraType(val id: Int) {
        LOG(1),
        KAFKA(2);

        companion object {
            fun fromId(id: Int): ExtraType = when (id) {
                1 -> LOG
                2 -> KAFKA
                else -> error("Unknown ExtraType id=$id")
            }
        }
    }

    private enum class DomainType(val id: Int) {
        LOG(1),
        KAFKA(2);

        companion object {
            fun of(line: DomainLine): DomainType = when (line) {
                is LogLineDomain -> LOG
                is KafkaLineDomain -> KAFKA
            }

            fun fromId(id: Int): DomainType = when (id) {
                1 -> LOG
                2 -> KAFKA
                else -> error("Unknown DomainType id=$id")
            }
        }

        fun levelFromOrdinal(ord: Int): LogLevel {
            val levels = LogLevel.entries
            return levels[min(ord, levels.lastIndex)]
        }

        fun build(
            seq: Long,
            level: LogLevel,
            timestamp: Long,
            message: String,
            indexIdentifier: String,
            extra: Any?,
        ): DomainLine {
            return when (this) {
                LOG -> {
                    val e =
                        extra as? DomainExtra.LogExtra ?: DomainExtra.LogExtra("", "", "", "", null, null, null, null)
                    LogLineDomain(
                        seq = seq,
                        level = level,
                        timestamp = timestamp,
                        message = message,
                        indexIdentifier = indexIdentifier,
                        threadName = e.threadName,
                        serviceName = e.serviceName,
                        serviceVersion = e.serviceVersion,
                        logger = e.logger,
                        correlationId = e.correlationId,
                        requestId = e.requestId,
                        errorMessage = e.errorMessage,
                        stacktrace = e.stacktrace,
                    )
                }

                KAFKA -> {
                    val e =
                        extra as? DomainExtra.KafkaExtra ?: DomainExtra.KafkaExtra("", null, 0L, 0, "", null, null, "")
                    KafkaLineDomain(
                        seq = seq,
                        level = level,
                        timestamp = timestamp,
                        message = message,
                        indexIdentifier = indexIdentifier,
                        topic = e.topic,
                        key = e.key,
                        offset = e.offset,
                        partition = e.partition,
                        headers = e.headers,
                        correlationId = e.correlationId,
                        requestId = e.requestId,
                        compositeEventId = e.compositeEventId,
                    )
                }
            }
        }
    }

    private sealed interface DomainExtra {
        data class LogExtra(
            val threadName: String,
            val serviceName: String,
            val serviceVersion: String,
            val logger: String,
            val correlationId: String?,
            val requestId: String?,
            val errorMessage: String?,
            val stacktrace: String?,
        ) : DomainExtra

        data class KafkaExtra(
            val topic: String,
            val key: String?,
            val offset: Long,
            val partition: Int,
            val headers: String,
            val correlationId: String?,
            val requestId: String?,
            val compositeEventId: String,
        ) : DomainExtra
    }

    private class StringDictionary(initialCapacity: Int = 16) {
        private val lock = Any()
        private val toId = HashMap<String, Int>(initialCapacity)
        private val fromId = ArrayList<String>(initialCapacity)


        fun idOf(s: String): Int {
            val existing = toId[s]
            if (existing != null) return existing
            synchronized(lock) {
                val existing2 = toId[s]
                if (existing2 != null) return existing2
                val id = fromId.size
                fromId.add(s)
                toId[s] = id
                return id
            }
        }

        private lateinit var frozenFromId: Array<String>

        fun freeze() {
            synchronized(lock) {
                fromId.trimToSize()
                frozenFromId = fromId.toTypedArray()
                toId.clear()
                fromId.clear()
            }
        }

        fun get(id: Int): String {
            return if (::frozenFromId.isInitialized) {
                frozenFromId[id]
            } else {
                fromId[id]
            }
        }
    }
    private fun normalizeUuidCandidate(s: String): String? {
        val t = s.trim().trim('"', '\'', ',', '.', ';', ':', ')', '(', ']', '[', '}', '{', '>')
        return if (UuidCodec.looksLikeUuid(t)) t else null
    }

    /** Encodes: 0 = null, 1 = uuidDict id, 2 = stringDict id */
    private fun FastByteWriter.writeNullableStringOrUuidId(s: String?) {
        if (s == null) {
            writeUVarInt(0)
            return
        }
        val u = normalizeUuidCandidate(s)
        if (u != null) {
            writeUVarInt(1)
            writeUVarInt(uuidDict.idOf(u))
        } else {
            writeUVarInt(2)
            writeUVarInt(stringDict.idOf(s))
        }
    }

    private fun FastByteReader.readNullableStringOrUuidId(): String? {
        return when (val tag = readUVarInt()) {
            0 -> null
            1 -> uuidDict.get(readUVarInt())
            2 -> stringDict.get(readUVarInt())
            else -> error("Bad nullableStringOrUuid tag=$tag")
        }
    }

    private fun FastByteWriter.writeNullableStringId(dict: StringDictionary, s: String?) {
        if (s == null) writeUVarInt(0) else writeUVarInt(dict.idOf(s) + 1)
    }

    private fun FastByteReader.readNullableStringId(dict: StringDictionary): String? {
        val idPlus = readUVarInt()
        if (idPlus == 0) return null
        return dict.get(idPlus - 1)
    }

    private class DrainMiner(
        private val maxDepth: Int,
        private val maxClustersPerLeaf: Int,
        private val maxChildrenPerNode: Int,
    ) {
        private val root = Node(depth = 0)
        private val idGen = AtomicLong(0)
        val clustersById = HashMap<Int, Cluster>(4096)
        fun learn(tokens: Array<String>): Cluster {
            val tokenCount = tokens.size
            val first = root.childrenByKey.getOrPut(tokenCount.toString()) { Node(1) }
            var node = first
            val depthLimit = min(maxDepth, tokenCount)
            for (d in 1 until depthLimit) {
                val key = routingKey(tokens[d])
                val next = node.childrenByKey[key]
                    ?: if (node.childrenByKey.size >= maxChildrenPerNode) {
                        node.childrenByKey.getOrPut(VAR) { Node(node.depth + 1) }
                    } else {
                        node.childrenByKey.getOrPut(key) { Node(node.depth + 1) }
                    }
                node = next
            }
            val clusters = node.clusters
            var best: Cluster? = null
            var bestScore = -1
            for (c in clusters) {
                val score = c.matchScore(tokens)
                if (score > bestScore) {
                    bestScore = score
                    best = c
                }
            }
            if (best != null && bestScore >= best.similarityThreshold) {
                best.update(tokens)
                return best
            }
            val id = idGen.incrementAndGet().toInt()
            val newCluster = Cluster(id, tokens.copyOf())
            clusters.add(newCluster)
            clustersById[id] = newCluster
            if (clusters.size > maxClustersPerLeaf) {
                var minIdx = 0
                var minSeen = Int.MAX_VALUE
                for (i in clusters.indices) {
                    val s = clusters[i].seen
                    if (s < minSeen) {
                        minSeen = s
                        minIdx = i
                    }
                }
                val removed = clusters.removeAt(minIdx)
                clustersById.remove(removed.id)
            }
            return newCluster
        }

        private fun routingKey(token: String): String = if (looksVariable(token)) VAR else token
        private fun looksVariable(token: String): Boolean {
            if (token.isEmpty()) return false
            var digits = 0
            for (i in token.indices) if (token[i] in '0'..'9') digits++
            if (digits >= 3) return true
            return UuidCodec.looksLikeUuid(token)
        }

        class Node(val depth: Int) {
            val childrenByKey: MutableMap<String, Node> = HashMap()
            val clusters: MutableList<Cluster> = ArrayList()
        }

        class Cluster(val id: Int, val templateTokens: Array<String>) {
            var seen: Int = 1
            val similarityThreshold: Int = (templateTokens.size * 0.6).toInt()
            fun matchScore(tokens: Array<String>): Int {
                var score = 0
                for (i in tokens.indices) {
                    val t = templateTokens[i]
                    if (t !== VAR && t == tokens[i]) score++
                }
                return score
            }

            fun update(tokens: Array<String>) {
                seen++
                for (i in tokens.indices) {
                    if (templateTokens[i] === VAR) continue
                    if (templateTokens[i] != tokens[i]) templateTokens[i] = VAR
                }
            }
        }

        companion object {
            const val VAR = "<*>"
        }
    }

    object MessageTokenizer {
        enum class Mode { Whitespace, LogLike }

        fun tokenize(message: String, mode: Mode): Array<String> {
            return when (mode) {
                Mode.Whitespace -> tokenizeWhitespace(message)
                Mode.LogLike -> tokenizeLogLike(message)
            }
        }

        fun join(tokens: Array<String>): String {
            if (tokens.isEmpty()) return ""
            val sb = StringBuilder(tokens.size * 8)
            for (t in tokens) sb.append(t)
            return sb.toString()
        }

        private fun tokenizeWhitespace(message: String): Array<String> {
            val out = ArrayList<String>(16)
            var i = 0
            val n = message.length
            while (i < n) {
                while (i < n && message[i].isWhitespace()) i++
                if (i >= n) break
                val start = i
                while (i < n && !message[i].isWhitespace()) i++
                out.add(message.substring(start, i))
                if (i < n) out.add(" ")
            }
            if (out.isNotEmpty() && out.last() == " ") out.removeLast()
            return out.toTypedArray()
        }

        private fun tokenizeLogLike(message: String): Array<String> {
            val out = ArrayList<String>(message.length / 4)
            val n = message.length
            var i = 0
            fun isDelimiter(c: Char): Boolean =
                c.isWhitespace() || c == '/' || c == '?' || c == '&' || c == '=' || c == ':'
            while (i < n) {
                val c = message[i]
                if (isDelimiter(c)) {
                    if (c.isWhitespace()) {
                        while (i < n && message[i].isWhitespace()) i++
                        if (out.isNotEmpty()) out.add(" ")
                        continue
                    }
                    out.add(c.toString())
                    i++
                    continue
                }
                val start = i
                while (i < n && !isDelimiter(message[i])) i++
                out.add(message.substring(start, i))
            }
            if (out.isNotEmpty() && out.last() == " ") out.removeLast()
            return out.toTypedArray()
        }
    }

    class FastByteWriter(initialCapacity: Int) {
        private var buf = ByteArray(maxOf(16, initialCapacity))
        private var pos = 0
        fun writeByte(b: Int) {
            val p = pos
            if (p == buf.size) buf = buf.copyOf(buf.size * 2)
            buf[p] = b.toByte()
            pos = p + 1
        }

        fun writeBytes(src: ByteArray) {
            ensure(src.size)
            src.copyInto(buf, destinationOffset = pos)
            pos += src.size
        }

        fun writeUVarInt(value: Int) {
            var v = value
            while ((v and 0x7f.inv()) != 0) {
                writeByte((v and 0x7f) or 0x80)
                v = v ushr 7
            }
            writeByte(v)
        }

        fun writeUVarLong(value: Long) {
            var v = value
            while ((v and 0x7fL.inv()) != 0L) {
                writeByte(((v and 0x7fL) or 0x80L).toInt())
                v = v ushr 7
            }
            writeByte(v.toInt())
        }

        fun writeZigZagVarLong(value: Long) {
            val zz = (value shl 1) xor (value shr 63)
            writeUVarLong(zz)
        }

        fun writeUtf8(s: String) {
            val b = s.toByteArray(Charsets.UTF_8)
            writeUVarInt(b.size)
            writeBytes(b)
        }

        fun toByteArray(): ByteArray = buf.copyOf(pos)
        private fun ensure(extra: Int) {
            val need = pos + extra
            if (need <= buf.size) return
            var newSize = buf.size
            while (newSize < need) newSize = newSize shl 1
            buf = buf.copyOf(newSize)
        }
    }

    class FastByteReader(private val buf: ByteArray, start: Int, end: Int) {
        private var pos = start
        private val limit = end

        fun readByte(): Int {
            if (pos >= limit) throw IndexOutOfBoundsException("read past end")
            return buf[pos++].toInt() and 0xff
        }

        fun readUVarInt(): Int {
            var x = 0
            var s = 0
            while (true) {
                val b = readByte()
                x = x or ((b and 0x7f) shl s)
                if ((b and 0x80) == 0) return x
                s += 7
            }
        }

        fun readUVarLong(): Long {
            var x = 0L
            var s = 0
            while (true) {
                val b = readByte()
                x = x or (((b and 0x7f).toLong()) shl s)
                if ((b and 0x80) == 0) return x
                s += 7
            }
        }

        fun readZigZagVarLong(): Long {
            val zz = readUVarLong()
            return (zz ushr 1) xor -(zz and 1L)
        }

        fun readUtf8(): String {
            val len = readUVarInt()
            if (len == 0) return ""
            val s = pos
            val e = s + len
            if (e > limit) throw IndexOutOfBoundsException("readUtf8 len=$len pos=$pos limit=$limit")
            pos = e
            return String(buf, s, len, Charsets.UTF_8)
        }
    }

    object UuidCodec {
        fun looksLikeUuid(s: String): Boolean {
            val clean = s.replace("-", "")
            return clean.length == 32 && clean.all { it.isDigit() || it.lowercaseChar() in 'a'..'f' }
        }

        fun writeUuid(out: FastByteWriter, s: String) {
            val bytes = ByteArray(16)
            var bi = 0
            var i = 0
            while (i < s.length) {
                if (s[i] == '-') {
                    i++
                    continue
                }
                val hi = hexVal(s[i])
                val lo = hexVal(s[i + 1])
                bytes[bi++] = ((hi shl 4) or lo).toByte()
                i += 2
            }
            out.writeBytes(bytes)
        }

        fun readUuid(inp: FastByteReader): String {
            val b = ByteArray(16)
            for (i in 0 until 16) b[i] = inp.readByte().toByte()
            return buildString(36) {
                for (i in 0 until 16) {
                    val v = b[i].toInt() and 0xff
                    append(HEX[v ushr 4])
                    append(HEX[v and 0x0f])
                    when (i) {
                        3, 5, 7, 9 -> append('-')
                    }
                }
            }
        }

        private fun hexVal(c: Char): Int = when (c) {
            in '0'..'9' -> c.code - '0'.code
            in 'a'..'f' -> c.code - 'a'.code + 10
            in 'A'..'F' -> c.code - 'A'.code + 10
            else -> 0
        }

        private val HEX = "0123456789abcdef".toCharArray()
    }

    private class LongArrayList(initialCapacity: Int = 16) {
        private var a = LongArray(maxOf(16, initialCapacity))
        var size: Int = 0
            private set

        fun add(v: Long) {
            val s = size
            if (s == a.size) a = a.copyOf(a.size * 2)
            a[s] = v
            size = s + 1
        }

        fun get(i: Int): Long = a[i]
    }

    enum class StorageKind { MAP, PACKED }

    fun isSealed(): Boolean = sealed.get()

    fun storageKind(): StorageKind =
        when (storageRef.get()) {
            is PackedStorage -> StorageKind.PACKED
            else -> StorageKind.MAP
        }

    fun packedByteSizeOrZero(): Int =
        (storageRef.get() as? PackedStorage)?.packedByteSize() ?: 0

    data class MemoryEstimate(
        val storageKind: StorageKind,
        val entries: Int,
        val payloadBytesSampled: Long,
        val estimatedPayloadBytesTotal: Long,
        val estimatedOverheadBytes: Long,
        val estimatedTotalBytes: Long,
        val sampleN: Int,
    ) {
        override fun toString(): String {
            return "storage=$storageKind entries=$entries sampleN=$sampleN estPayload=$estimatedPayloadBytesTotal estOverhead=$estimatedOverheadBytes estTotal=$estimatedTotalBytes"
        }
    }

    fun estimateMemoryUsage(sampleMaxEntries: Int = 50_000): MemoryEstimate {
        return when (val s = storageRef.get()) {
            is PackedStorage -> {
                val entries = s.size()
                val payload = s.packedByteSize().toLong()
                val idsBytes = entries.toLong() * 8L
                val offsetsBytes = (entries.toLong() + 1L) * 4L
                val overhead = idsBytes + offsetsBytes
                MemoryEstimate(
                    storageKind = StorageKind.PACKED,
                    entries = entries,
                    payloadBytesSampled = payload,
                    estimatedPayloadBytesTotal = payload,
                    estimatedOverheadBytes = overhead,
                    estimatedTotalBytes = payload + overhead,
                    sampleN = entries,
                )
            }

            else -> {
                val entries = s.size()
                val sample = s.sampleEntries(sampleMaxEntries)
                val sampleN = sample.size.coerceAtLeast(1)

                var payloadSample = 0L
                for (i in sample.indices) payloadSample += sample[i].second.size.toLong()

                val avg = payloadSample.toDouble() / sampleN.toDouble()
                val estPayloadTotal = (avg * entries.toDouble()).toLong()

                val entryOverhead = 96L
                val estimatedOverhead = entryOverhead * entries.toLong()

                MemoryEstimate(
                    storageKind = StorageKind.MAP,
                    entries = entries,
                    payloadBytesSampled = payloadSample,
                    estimatedPayloadBytesTotal = estPayloadTotal,
                    estimatedOverheadBytes = estimatedOverhead,
                    estimatedTotalBytes = estPayloadTotal + estimatedOverhead,
                    sampleN = sampleN,
                )
            }
        }
    }
}