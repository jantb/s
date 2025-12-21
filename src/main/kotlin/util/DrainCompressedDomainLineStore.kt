package util

import LogLevel
import app.DomainLine
import app.KafkaLineDomain
import app.LogLineDomain
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

class DrainCompressedDomainLineStore(
    scope: CoroutineScope,
    private val maxDepth: Int = 4,
    private val maxClustersPerLeaf: Int = 64,
    private val maxChildrenPerNode: Int = 128,
) {
    private val idGen = AtomicLong(0)
    private val bytesById = ConcurrentHashMap<Long, ByteArray>(1 shl 16)

    private val baseTimestamp = AtomicLong(0L)

    private val templatesById = ConcurrentHashMap<Int, Array<String>>(4096)
    private val miner = DrainMiner(
        maxDepth = maxDepth,
        maxClustersPerLeaf = maxClustersPerLeaf,
        maxChildrenPerNode = maxChildrenPerNode,
    )
    private val writeCh = Channel<PutReq>(capacity = Channel.BUFFERED)
    private val stringDict = StringDictionary(initialCapacity = 1 shl 14)

    init {
        scope.launch(Dispatchers.Default + CoroutineName("DrainCompressedDomainLineStore-writer")) {
            for (req in writeCh) {
                val id = req.line.seq
                val last = idGen.get()
                if (id > last) idGen.set(id)

                val bts = baseTimestamp.get()
                if (bts == 0L) baseTimestamp.compareAndSet(0L, req.line.timestamp)

                bytesById[id] = encodeToBytes(line = req.line)
                req.result.complete(id)
            }
        }
    }

    suspend fun put(line: DomainLine): Long {
        val d = CompletableDeferred<Long>()
        writeCh.send(PutReq(line, d))
        return d.await()
    }

    fun getBytesBlocking(id: Long): ByteArray? = bytesById[id]

    fun getLineBlocking(id: Long): DomainLine? {
        val bytes = bytesById[id] ?: return null
        val line = decodeFull(bytes, id = id) ?: return null
        return line
    }

    private fun encodeToBytes(line: DomainLine): ByteArray {
        val domainType = DomainType.of(line)

        val msgTokens = MessageTokenizer.tokenize(line.message, MessageTokenizer.Mode.LogLike)
        val cluster = miner.learn(msgTokens)
        val templateId = cluster.id

        templatesById[templateId] = cluster.templateTokens

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
                out.writeNullableStringId(stringDict, line.correlationId)
                out.writeNullableStringId(stringDict, line.requestId)
                out.writeNullableStringId(stringDict, line.errorMessage)
                out.writeNullableStringId(stringDict, line.stacktrace)
            }

            is KafkaLineDomain -> {
                out.writeUVarInt(1)
                out.writeUVarInt(ExtraType.KAFKA.id)
                out.writeUVarInt(stringDict.idOf(line.topic))
                out.writeNullableStringId(stringDict, line.key)
                out.writeZigZagVarLong(line.offset)
                out.writeZigZagVarLong(line.partition.toLong())
                out.writeUVarInt(stringDict.idOf(line.headers))
                out.writeNullableStringId(stringDict, line.correlationId)
                out.writeNullableStringId(stringDict, line.requestId)
                out.writeUVarInt(stringDict.idOf(line.compositeEventId))
            }
        }

        for (v in vars) encodeVariable(out, v)
        return out.toByteArray()
    }

    private fun decodeFull(bytes: ByteArray, id: Long): DomainLine? {
        val inp = FastByteReader(bytes)

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

        val templateTokens = templatesById[templateId] ?: return null
        val tokens = templateTokens.copyOf()

        for (i in 0 until varCount) {
            val pos = positions[i]
            val value = decodeVariable(inp)
            if (pos in tokens.indices) tokens[pos] = value
        }

        val message = MessageTokenizer.join(tokens)

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
                correlationId = inp.readNullableStringId(stringDict),
                requestId = inp.readNullableStringId(stringDict),
                errorMessage = inp.readNullableStringId(stringDict),
                stacktrace = inp.readNullableStringId(stringDict),
            )

            ExtraType.KAFKA -> DomainExtra.KafkaExtra(
                topic = stringDict.get(inp.readUVarInt()),
                key = inp.readNullableStringId(stringDict),
                offset = inp.readZigZagVarLong(),
                partition = inp.readZigZagVarLong().toInt(),
                headers = stringDict.get(inp.readUVarInt()),
                correlationId = inp.readNullableStringId(stringDict),
                requestId = inp.readNullableStringId(stringDict),
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
        if (UuidCodec.looksLikeUuid(raw)) {
            out.writeUVarInt(VarTag.UUID.id)
            UuidCodec.writeUuid(out, raw)
            return
        }
        out.writeUVarInt(VarTag.STRING.id)
        out.writeUtf8(raw)
    }

    private fun decodeVariable(inp: FastByteReader): String {
        return when (VarTag.fromId(inp.readUVarInt())) {
            VarTag.LONG -> inp.readZigZagVarLong().toString()
            VarTag.UUID -> UuidCodec.readUuid(inp)
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
                    val e = extra as? DomainExtra.LogExtra ?: DomainExtra.LogExtra("", "", "", "", null, null, null, null)
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
                    val e = extra as? DomainExtra.KafkaExtra ?: DomainExtra.KafkaExtra("", null, 0L, 0, "", null, null, "")
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

    private class LruCache<K, V>(private val maxEntries: Int) {
        private val map = object : LinkedHashMap<K, V>(maxEntries, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean = size > maxEntries
        }

        fun get(key: K): V? = map[key]
        fun put(key: K, value: V) {
            map[key] = value
        }

        fun clear() = map.clear()
    }

    private class StringDictionary(initialCapacity: Int = 16) {
        private val lock = Any()
        private val toId = ConcurrentHashMap<String, Int>(initialCapacity)
        private val fromId = ArrayList<String>(initialCapacity)

        init {
            fromId.add("")
            toId[""] = 0
        }

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

        fun get(id: Int): String {
            synchronized(lock) {
                return fromId[id]
            }
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
        private val clustersById = HashMap<Int, Cluster>(4096)

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

    /**
     * Tokenizer used by DrainCompressedDomainLineStore to improve template reuse.
     *
     * We keep delimiters as tokens so join() can reconstruct the original string exactly.
     */
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
                if (i < n) out.add(" ") // preserve spaces between words
            }
            // If message started/ended with spaces, we drop them (same behavior as previous Tokenizer)
            if (out.isNotEmpty() && out.last() == " ") out.removeLast()
            return out.toTypedArray()
        }

        /**
         * Splits on:
         * - whitespace
         * - '/', '?', '&', '=', ':'
         *
         * Keeps delimiters as tokens, including single spaces, so join() is exact.
         */
        private fun tokenizeLogLike(message: String): Array<String> {
            val out = ArrayList<String>(message.length / 4)
            val n = message.length
            var i = 0

            fun isDelimiter(c: Char): Boolean =
                c.isWhitespace() || c == '/' || c == '?' || c == '&' || c == '=' || c == ':'

            while (i < n) {
                val c = message[i]

                if (isDelimiter(c)) {
                    // normalize any whitespace run to a single space token (keeps join deterministic)
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

            // remove trailing space token if any
            if (out.isNotEmpty() && out.last() == " ") out.removeLast()
            return out.toTypedArray()
        }
    }

    private class FastByteWriter(initialCapacity: Int) {
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

    private class FastByteReader(private val buf: ByteArray) {
        private var pos = 0

        fun readByte(): Int = buf[pos++].toInt() and 0xff

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
            val start = pos
            val end = start + len
            if (end > buf.size) throw IndexOutOfBoundsException("readUtf8 len=$len pos=$pos size=${buf.size}")

            var i = start
            while (i < end) {
                if (buf[i].toInt() and 0x80 != 0) {
                    val s = String(buf, start, len, Charsets.UTF_8)
                    pos = end
                    return s
                }
                i++
            }

            val chars = CharArray(len)
            i = 0
            var p = start
            while (i < len) {
                chars[i] = (buf[p++].toInt() and 0xff).toChar()
                i++
            }
            pos = end
            return String(chars)
        }
    }

    private object UuidCodec {
        fun looksLikeUuid(s: String): Boolean {
            if (s.length != 36) return false
            return s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-'
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
}