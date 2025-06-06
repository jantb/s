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
    val logLevel: LogLevel
)

enum class DomainLineType {
    LOG, KAFKA
}

data class FinalizedPositionInfo(
    val block: Int,
    val posInGrouped: List<Int>,
    val domainLineType: DomainLineType,
    val logLevel: LogLevel,
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

        private fun mapToToken(s: String): Token {
            return if (isPotentialUUID(s)) {
                try {
                    val uuid = UUID.fromString(s)
                    Token.UUIDValue(uuid)
                } catch (e: IllegalArgumentException) {
                    s.toLongOrNull()?.let {
                        // Store the Long value in VarInt and return a Number token with the index
                        Token.Number(it)
                    } ?: Token.StringValue(s)
                }
            } else {
                s.toLongOrNull()?.let {
                    // Store the Long value in VarInt and return a Number token with the index
                    Token.Number(it)
                } ?: Token.StringValue(s)
            }
        }

        private fun isPotentialUUID(s: String): Boolean {
            return s.length == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-'
        }

        private fun getStringFromToken(token: Token): String = when (token) {
            is Token.StringValue -> token.value
            is Token.UUIDValue -> token.value.toString()
            is Token.Number -> token.value.toInt().toString()
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
                    logLevel = posInfo.logLevel,
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
                                "${getStringFromToken(last.first())} ${
                                    getStringFromToken(
                                        tokenVec.first(),
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
            severityNumbers[it.block] = it.logLevel.ordinal.toByte()
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
        val tokens = line.split(" ").map { mapToToken(it) }
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
                logLevel = when (logLine) {
                    is KafkaLineDomain -> LogLevel.KAFKA
                    is LogLineDomain -> logLine.level
                },
            )
        )

        return positionInfo.size - 1
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
                block
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
                logLevel = LogLevel.UNKNOWN,
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