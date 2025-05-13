import app.Domain
import app.DomainLine
import app.KafkaLineDomain
import app.LogLineDomain
import util.VarInt
import java.util.*
data class Signature(
    val typeSequence: List<Byte> = emptyList()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Signature) return false
        return typeSequence == other.typeSequence
    }

    override fun hashCode(): Int = typeSequence.hashCode()
}


sealed class Token {
    data class StringValue(val value: String) : Token()
    data class UUIDValue(val value: UUID) : Token()
    data class Number(val value: Long) : Token()
}

sealed class MultiToken {
    data class StringValue(val values: List<String>) : MultiToken()
    data class UUIDValue(val values: List<UUID>) : MultiToken()
    data class Number(val values: List<Long>) : MultiToken() // Simplified; assumes T64 is a list of numbers
}

data class PositionInfo(
    val signature: Signature,
    val posInGrouped: Int,
    val domainLineType: DomainLineType,
    val indexes: VarInt = VarInt(),
    val domainLine: DomainLine?
)

enum class DomainLineType {
    LOG, KAFKA
}

data class FinalizedPositionInfo(
    val block: Int,
    val posInGrouped: List<Int>,
    val domainLineType: DomainLineType,
    val domainLine: DomainLine?,
    val indexes: VarInt = VarInt()
)

class DrainTree(val indexIdentifier: String = "") {
    private lateinit var multiTokens: List<List<MultiToken>>
    private var grouped: MutableMap<Signature, MutableList<List<Token>>> = HashMap()
    private var positionInfo: MutableList<PositionInfo> = ArrayList()
    private var positionInfoHashMap: MutableMap<Signature, MutableList<Int>> = HashMap()
    private var finalizedPositionInfo: List<FinalizedPositionInfo> = ArrayList()
    private var logClusters: MutableList<LogCluster> = mutableListOf()
    private val store = false

    private val seqStore = VarInt()
    private val logLevelStore = VarInt()
    private val timestampStore = VarInt()
    private val offsetStore = VarInt()
    private val partitionStore = VarInt()
    private val numberTokenStore = VarInt()
    private val stringInterner = StringInterner()

    companion object {
        private fun generateSignature(tokens: List<Token>, logLevel: LogLevel): Signature {
            val typeSequence = tokens.map { token ->
                when (token) {
                    is Token.StringValue -> 1
                    is Token.UUIDValue -> 2
                    is Token.Number -> 3
                }.toByte()
            }.toMutableList()
            typeSequence.add(logLevel.ordinal.toByte())
            return Signature(typeSequence)
        }

        private fun mapToToken(s: String, numberStore: VarInt): Token {
            return if (isPotentialUUID(s)) {
                try {
                    val uuid = UUID.fromString(s)
                    Token.UUIDValue(uuid)
                } catch (e: IllegalArgumentException) {
                    s.toLongOrNull()?.let {
                        // Store the Long value in VarInt and return a Number token with the index
                        val index = numberStore.add(it).toLong()
                        Token.Number(index)
                    } ?: Token.StringValue(s)
                }
            } else {
                s.toLongOrNull()?.let {
                    // Store the Long value in VarInt and return a Number token with the index
                    val index = numberStore.add(it).toLong()
                    Token.Number(index)
                } ?: Token.StringValue(s)
            }
        }

        private fun isPotentialUUID(s: String): Boolean {
            return s.length == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-'
        }

        private fun getStringFromToken(token: Token, numberStore: VarInt): String = when (token) {
            is Token.StringValue -> token.value
            is Token.UUIDValue -> token.value.toString()
            is Token.Number -> numberStore.get(token.value.toInt()).toString()
        }

        private fun generateMultiTokens(tokenStorage: List<List<List<Token>>>): List<List<MultiToken>> {
            return tokenStorage.map { tokenVector ->
                tokenVector.map { tokens ->
                    when (tokens.first()) {
                        is Token.StringValue -> MultiToken.StringValue(
                            tokens.map { (it as Token.StringValue).value }
                        )

                        is Token.UUIDValue -> MultiToken.UUIDValue(
                            tokens.map { (it as Token.UUIDValue).value }
                        )

                        is Token.Number -> MultiToken.Number(
                            tokens.map { (it as Token.Number).value }
                        )
                    }
                }
            }
        }

        private fun mergeVectorOfTokens(
            vectorOfTokens: List<List<Token>>,
            positionInfos: List<PositionInfo>,
            block: Int,
            numberStore: VarInt
        ): Pair<List<FinalizedPositionInfo>, List<List<Token>>> {
            val uniques: List<MutableMap<Token, Int>> = vectorOfTokens.first().map { HashMap() }
            vectorOfTokens.forEach { vec ->
                vec.forEachIndexed { index, token ->
                    uniques[index].putIfAbsent(token, uniques[index].size)
                }
            }

            val uniquesVec: List<MutableList<Token>> = vectorOfTokens.first().map { ArrayList() }
            uniques.forEachIndexed { index, map ->
                val pairs = map.entries.sortedBy { it.value }
                pairs.forEach { (token, _) -> uniquesVec[index].add(token) }
            }

            val finalPos = positionInfos.map { posInfo ->
                val posInGrouped = vectorOfTokens[posInfo.posInGrouped]
                FinalizedPositionInfo(
                    block = block,
                    posInGrouped = posInGrouped.mapIndexed { index, token ->
                        uniques[index][token] ?: throw IllegalStateException("Token not found")
                    },
                    domainLineType = posInfo.domainLineType,
                    indexes = posInfo.indexes,
                    domainLine = posInfo.domainLine,
                )
            }

            val mergedVecs = ArrayList<List<Token>>()
            val pos: List<MutableList<Int?>> = finalPos.map { it.posInGrouped.map { pos -> pos }.toMutableList() }
            uniquesVec.forEachIndexed { index, tokenVec ->
                if (mergedVecs.isEmpty()) {
                    mergedVecs.add(tokenVec)
                    return@forEachIndexed
                }
                if (mergedVecs.last().size == 1 && tokenVec.size == 1) {
                    val last = mergedVecs.removeLast()
                    mergedVecs.add(
                        listOf(
                            Token.StringValue(
                                "${getStringFromToken(last.first(), numberStore)} ${
                                    getStringFromToken(
                                        tokenVec.first(),
                                        numberStore
                                    )
                                }"
                            )
                        )
                    )
                    pos.forEach { it[index] = null }
                } else {
                    mergedVecs.add(tokenVec)
                }
            }

            val updatedFinalPos = finalPos.mapIndexed { index, fin ->
                fin.copy(
                    posInGrouped = pos[index].filterNotNull()
                )
            }

            return updatedFinalPos to mergedVecs
        }
    }

    fun logClusters(): List<LogCluster> {
        return if (logClusters.isEmpty()) {
            val (finalized, tokenStorage) = mergeGroupedTokens()
            getLogClusters(generateMultiTokens(tokenStorage), finalized)
        } else {
            logClusters
        }
    }

    private fun getLogClusters(
        logStorage: List<List<MultiToken>>,
        positionInfo: List<FinalizedPositionInfo>
    ): List<LogCluster> {
        val matches = LongArray(logStorage.size)
        val severityNumbers = ByteArray(logStorage.size)
        positionInfo.forEach {
            matches[it.block]++
            severityNumbers[it.block] = logLevelStore.get(it.indexes.get(1).toInt()).toByte()
        }

        val result = logStorage.map { vec ->
            vec.joinToString(" ") { multiToken ->
                when (multiToken) {
                    is MultiToken.StringValue -> if (multiToken.values.size == 1) {
                        multiToken.values.first()
                    } else {
                        "<string>"
                    }

                    is MultiToken.UUIDValue -> "<uuid>"
                    is MultiToken.Number -> "<number>"
                }
            }
        }

        return matches.indices.map { i ->
            LogCluster(matches[i] + 1, LogLevel.entries[severityNumbers[i].toInt()], result[i], indexIdentifier)
        }
    }

    fun add(logLine: DomainLine): Int {
        val line = logLine.message
        val tokens = line.split(" ").map { mapToToken(it, numberTokenStore) } // Pass numberTokenStore
        val signature = generateSignature(tokens, logLine.level)
        val entry = grouped.getOrPut(signature) { ArrayList() }
        entry.add(tokens)
        val positionIndices = positionInfoHashMap.getOrPut(signature) { ArrayList() }
        positionIndices.add(positionInfo.size)
        positionInfo.add(
            PositionInfo(
                signature = signature,
                posInGrouped = entry.size - 1,
                domainLineType = when (logLine) {
                    is KafkaLineDomain -> DomainLineType.KAFKA
                    is LogLineDomain -> DomainLineType.LOG
                },
                domainLine = if(store)null else logLine,
            ).also {
                it.indexes.add(seqStore.add(logLine.seq).toLong())
                it.indexes.add(logLevelStore.add(logLine.level.ordinal.toLong()).toLong())
                it.indexes.add(timestampStore.add(logLine.timestamp).toLong())
                it.indexes.add(stringInterner.intern(logLine.indexIdentifier).toLong())
                when (logLine) {
                    is KafkaLineDomain -> {
                        it.indexes.add(stringInterner.intern(logLine.topic).toLong())
                        it.indexes.add(stringInterner.intern(logLine.key).toLong())
                        it.indexes.add(offsetStore.add(logLine.offset).toLong())
                        it.indexes.add(partitionStore.add(logLine.partition.toLong()).toLong())
                        it.indexes.add(stringInterner.intern(logLine.headers).toLong())
                        it.indexes.add(stringInterner.intern(logLine.correlationId).toLong())
                        it.indexes.add(stringInterner.intern(logLine.requestId).toLong())
                        it.indexes.add(stringInterner.intern(logLine.compositeEventId).toLong())
                    }

                    is LogLineDomain -> {
                        it.indexes.add(stringInterner.intern(logLine.threadName).toLong())
                        it.indexes.add(stringInterner.intern(logLine.serviceName).toLong())
                        it.indexes.add(stringInterner.intern(logLine.serviceVersion).toLong())
                        it.indexes.add(stringInterner.intern(logLine.logger).toLong())
                        it.indexes.add(stringInterner.intern(logLine.correlationId).toLong())
                        it.indexes.add(stringInterner.intern(logLine.requestId).toLong())
                        it.indexes.add(stringInterner.intern(logLine.errorMessage).toLong())
                        it.indexes.add(stringInterner.intern(logLine.stacktrace).toLong())
                    }
                }

            }
        )

        return positionInfo.size - 1
    }

    fun get(index: Int): DomainLine {
        return if (finalizedPositionInfo.isEmpty()) {
            val positionInfo = positionInfo[index]
            if (!store) {
                return positionInfo.domainLine!!
            } else {
                val message = grouped[positionInfo.signature]!![positionInfo.posInGrouped].joinToString(" ") {
                    when (it) {
                        is Token.StringValue -> it.value
                        is Token.UUIDValue -> it.value.toString()
                        is Token.Number -> it.value.toString()
                    }
                }
                return when (positionInfo.domainLineType) {
                    DomainLineType.LOG -> {
                        LogLineDomain(
                            seq = seqStore.get(positionInfo.indexes.get(0).toInt()),
                            level = LogLevel.fromOrdinal(
                                logLevelStore.get(positionInfo.indexes.get(1).toInt()).toInt()
                            ),
                            timestamp = timestampStore.get(positionInfo.indexes.get(2).toInt()),
                            message = message,
                            indexIdentifier = stringInterner.get(positionInfo.indexes.get(3).toInt())!!,
                            threadName = stringInterner.get(positionInfo.indexes.get(4).toInt())!!,
                            serviceName = stringInterner.get(positionInfo.indexes.get(5).toInt())!!,
                            serviceVersion = stringInterner.get(positionInfo.indexes.get(5).toInt())!!,
                            logger = stringInterner.get(positionInfo.indexes.get(6).toInt())!!,
                            correlationId = stringInterner.get(positionInfo.indexes.get(7).toInt()),
                            requestId = stringInterner.get(positionInfo.indexes.get(8).toInt()),
                            errorMessage = stringInterner.get(positionInfo.indexes.get(9).toInt()),
                            stacktrace = stringInterner.get(positionInfo.indexes.get(10).toInt()),
                        )
                    }

                    DomainLineType.KAFKA -> {
                        KafkaLineDomain(
                            seq = seqStore.get(positionInfo.indexes.get(0).toInt()),
                            level = LogLevel.fromOrdinal(
                                logLevelStore.get(positionInfo.indexes.get(1).toInt()).toInt()
                            ),
                            timestamp = timestampStore.get(positionInfo.indexes.get(2).toInt()),
                            message = message,
                            indexIdentifier = stringInterner.get(positionInfo.indexes.get(3).toInt())!!,
                            topic = stringInterner.get(positionInfo.indexes.get(4).toInt())!!,
                            key = stringInterner.get(positionInfo.indexes.get(4).toInt()),
                            offset = offsetStore.get(positionInfo.indexes.get(5).toInt()),
                            partition = partitionStore.get(positionInfo.indexes.get(6).toInt()).toInt(),
                            headers = stringInterner.get(positionInfo.indexes.get(7).toInt())!!,
                            correlationId = stringInterner.get(positionInfo.indexes.get(8).toInt())!!,
                            requestId = stringInterner.get(positionInfo.indexes.get(9).toInt())!!,
                            compositeEventId = stringInterner.get(positionInfo.indexes.get(10).toInt())!!,
                        )
                    }
                }
            }

        } else {
            val positionInfo = finalizedPositionInfo[index]
            if (!store) {
                return positionInfo.domainLine!!
            } else {
                //
                val message = finalizedPositionInfo[index].posInGrouped.mapIndexed { i, pos ->
                    when (val token = multiTokens[finalizedPositionInfo[index].block][i]) {
                        is MultiToken.StringValue -> token.values[pos]
                        is MultiToken.UUIDValue -> token.values[pos].toString()
                        is MultiToken.Number -> token.values[pos].toString()
                    }
                }.joinToString(" ")
                return when (positionInfo.domainLineType) {
                    DomainLineType.LOG -> LogLineDomain(
                        seq = seqStore.get(positionInfo.indexes.get(0).toInt()),
                        level = LogLevel.fromOrdinal(logLevelStore.get(positionInfo.indexes.get(1).toInt()).toInt()),
                        timestamp = timestampStore.get(positionInfo.indexes.get(2).toInt()),
                        message = message,
                        indexIdentifier = stringInterner.get(positionInfo.indexes.get(3).toInt())!!,
                        threadName = stringInterner.get(positionInfo.indexes.get(4).toInt())!!,
                        serviceName = stringInterner.get(positionInfo.indexes.get(5).toInt())!!,
                        serviceVersion = stringInterner.get(positionInfo.indexes.get(5).toInt())!!,
                        logger = stringInterner.get(positionInfo.indexes.get(6).toInt())!!,
                        correlationId = stringInterner.get(positionInfo.indexes.get(7).toInt()),
                        requestId = stringInterner.get(positionInfo.indexes.get(8).toInt()),
                        errorMessage = stringInterner.get(positionInfo.indexes.get(9).toInt()),
                        stacktrace = stringInterner.get(positionInfo.indexes.get(10).toInt()),
                    )

                    DomainLineType.KAFKA -> KafkaLineDomain(
                        seq = seqStore.get(positionInfo.indexes.get(0).toInt()),
                        level = LogLevel.fromOrdinal(logLevelStore.get(positionInfo.indexes.get(1).toInt()).toInt()),
                        timestamp = timestampStore.get(positionInfo.indexes.get(2).toInt()),
                        message = message,
                        indexIdentifier = stringInterner.get(positionInfo.indexes.get(3).toInt())!!,
                        topic = stringInterner.get(positionInfo.indexes.get(4).toInt())!!,
                        key = stringInterner.get(positionInfo.indexes.get(4).toInt()),
                        offset = offsetStore.get(positionInfo.indexes.get(5).toInt()),
                        partition = partitionStore.get(positionInfo.indexes.get(6).toInt()).toInt(),
                        headers = stringInterner.get(positionInfo.indexes.get(7).toInt())!!,
                        correlationId = stringInterner.get(positionInfo.indexes.get(8).toInt())!!,
                        requestId = stringInterner.get(positionInfo.indexes.get(9).toInt())!!,
                        compositeEventId = stringInterner.get(positionInfo.indexes.get(10).toInt())!!,
                    )
                }
            }
        }
    }

    fun final() {
        val (finalizedInfo, tokenStorage) = mergeGroupedTokens()
        positionInfoHashMap.clear()
        this.multiTokens = generateMultiTokens(tokenStorage)
        this.finalizedPositionInfo = finalizedInfo
        this.logClusters.addAll(getLogClusters(multiTokens, finalizedInfo))
    }

    private fun mergeGroupedTokens(): Pair<List<FinalizedPositionInfo>, List<List<List<Token>>>> {
        val finalPositions = ArrayList<FinalizedPositionInfo>()
        val uniqueTokens = ArrayList<List<List<Token>>>()

        grouped.entries.forEachIndexed { block, (signature, vectorOfTokens) ->
            val positionsForSignature = positionInfoHashMap[signature]!!.map { positionInfo[it] }
            val (finalPos, uniqueVec) = mergeVectorOfTokens(
                vectorOfTokens,
                positionsForSignature,
                block,
                numberTokenStore
            )
            finalPositions.addAll(finalPos)
            uniqueTokens.add(uniqueVec)
        }

        val finalizedPositions = MutableList(positionInfo.size) {
            FinalizedPositionInfo(
                block = 0,
                posInGrouped = emptyList(),
                domainLineType = DomainLineType.LOG,
                indexes = VarInt(),
                domainLine = null
            )
        }

        grouped.keys.flatMap { signature ->
            positionInfoHashMap[signature]!!.mapIndexed { index, pos -> index to pos }
        }.forEach { (count, position) ->
            finalizedPositions[position] = finalPositions[count]
        }

        return finalizedPositions to uniqueTokens
    }
}

class StringInterner {
    private val strings = mutableListOf<String?>()
    private val indexMap = mutableMapOf<String?, Int>()

    fun intern(string: String?): Int {
        return indexMap.getOrPut(string) {
            strings.add(string)
            strings.size - 1
        }
    }

    fun get(index: Int): String? {
        return strings[index]
    }
}

data class LogCluster(val count: Long, val level: LogLevel, val block: String, val indexIdentifier: String)