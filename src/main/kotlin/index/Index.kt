package index

import index.Bf.Companion.estimate
import net.openhft.hashing.LongHashFunction
import java.io.Serializable
import java.util.*
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow

class Index(
    private var shards: MutableMap<Int, Shard> = HashMap(),
    var size: Int = 0,
    private val probability: Double = 0.03,
    occurrences: Map<String, Int> = emptyMap(),
    numberOfDocuments: Int = 0,
    targetSNR: Double = occurrences.values.map { calculateSNR(it, numberOfDocuments) }.average(),
    private val hashes: Map<String, Int> = occurrences.mapValues { stringIntEntry ->
        calculateRequiredHashes(
            currentSNR = calculateSNR(
                stringIntEntry.value, numberOfDocuments
            ),
            targetSNR = targetSNR,
        )
    },
) : Serializable {
    fun add(key: Int, value: String) {
        if (value.isBlank()) {
            return
        }

        val grams = value.grams()
        val estimate = estimate(grams.size, probability)

        shards.computeIfAbsent(estimate) {
            Shard(estimate, hashesMap = hashes)
        }.add(grams, key)
        size++
    }

    fun search(value: String): Sequence<Int> {
        val grams = value.grams()
        return shards.values.asSequence().map {
            it.search(grams)
        }.flatten().sortedByDescending { it }
    }

    fun searchNoSort(value: String): Set<Int> {
        val grams = value.grams()
        return shards.values.map {
            it.search(grams)
        }.flatten().toSet()
    }


    fun searchMustInclude(valueList: List<String>): List<Int> {
        if (valueList.isEmpty()) {
            return emptyList()
        }
        val res = mutableSetOf<Int>()
        res += searchNoSort(valueList.first())
        (1 until valueList.size).forEach {
            res.retainAll(searchNoSort(valueList[it]))
        }
        return res.sortedByDescending { it }
    }

    fun convertToHigherRank(goalCardinality: Double = 0.36) {
        shards.forEach { it.value.convertToHigherRankRows(goalCardinality) }
    }
}

class Shard(
    m: Int,
    private val valueList: MutableList<Int> = mutableListOf(),
    private val bitSets: Array<Row> = Array(m) { Row() },
    private val bf: Bf = Bf(m),
    private val hashesMap: Map<String, Int>,
    private var isHigherRow: Boolean = false
) : Serializable {

    fun add(gramList: List<String>, key: Int) {
        gramList.forEach { g ->
            bf.bitSet.clear()
            bf.add(g, hashesMap.getOrDefault(g, 1))
            bf.bitSet.getAllSetBitPositions().forEach {
                bitSets[it].setBit(valueList.size)
            }
        }
        valueList.add(key)
    }

    fun convertToHigherRankRows(goalCardinality: Double) {
        require(!isHigherRow) {
            "Can not convert an already converted higher row"
        }
        isHigherRow = true
        bitSets.forEach { it.expandToFitBit(valueList.size) }

        bitSets.forEach {
            it.convertToTargetDensity(goalCardinality)
        }
    }

    fun search(grams: List<String>): List<Int> {
        if (grams.isEmpty()) {
            return valueList.subList((valueList.lastIndex - 10_000).coerceIn(valueList.indices), valueList.size)
        }

        bf.bitSet.clear()
        grams.forEach {
            bf.add(it, hashesMap.getOrDefault(it, 1))
        }

        val rows =
            andBitsetsOfNotEqualLength(bf.bitSet.getAllSetBitPositions().map { bitSets[it] })
        if (rows.first.isEmpty()) {
            return emptyList()
        }
        return BitSet.valueOf(rows.first.words).getAllSetBitPositions().asSequence()
            .map {
                (0 until (1 shl rows.second.rank))
                    .map { i -> it + rows.second.rowLength * i * Long.SIZE_BITS }
                    .filter { it < valueList.size }
                    .map { valueList[it] }
            }.flatten().toList()
    }

    private var res = MutableBitset()
    private fun andBitsetsOfNotEqualLength(
        relevantBitsets: List<Row>,
    ): Pair<MutableBitset, RankAndRowLength> {
        res.clear()
        val rows = relevantBitsets.sortedByDescending { it.rank }
        res.or(MutableBitset(rows[0].bitArray))
        var currentRank = rows[0].rank

        for (i in 1 until rows.size) {
            if (res.isEmpty()) {
                return res to RankAndRowLength(rank = currentRank, rows.last().bitArray.size)
            }
            val row = rows[i]
            if (row.rank != currentRank) {
                while (currentRank != row.rank) {
                    res.appendCopy(row.bitArray.size)
                    currentRank--
                }
            }
            res.and(MutableBitset(row.bitArray))
        }

        return res to RankAndRowLength(rank = currentRank, rows.last().bitArray.size)
    }
}

class RankAndRowLength(val rank: Int, val rowLength: Int)

class Bf(private val m: Int) : Serializable {
    val bitSet = BitSet(m)
    fun add(s: String, hashes: Int) {
        if (hashes == 0) {
            return
        }
        (1..hashes).forEach {
            bitSet.set(LongHashFunction.xx3(it.toLong()).hashChars(s) mod m)
        }
    }

    private infix fun Long.mod(m: Int): Int {
        return Math.floorMod(this, m)
    }

    companion object {
        fun estimate(n: Int, p: Double): Int {
            val roundUpto = 2048
            return (ceil((n * ln(p)) / ln(1.0 / 2.0.pow(ln(2.0)))).toInt() + roundUpto - 1) / roundUpto * roundUpto
        }
    }
}

fun calculateSNR(occurrences: Int, totalDocuments: Int): Double {
    require(totalDocuments > 0)
    if (occurrences == totalDocuments) {
        return 1.0
    }
    val signal = occurrences.toDouble()
    val noise = (totalDocuments - occurrences).toDouble()

    return signal / noise
}

fun calculateRequiredHashes(currentSNR: Double, targetSNR: Double): Int {
    return when {
        currentSNR < targetSNR / 2 -> 3
        currentSNR < targetSNR -> 2
        currentSNR > 0.9 -> 1
        else -> 1
    }
}

fun String.grams(): List<String> {
    val lowercase = lowercase()
    if (lowercase.length < 3) {
        return listOf(lowercase)
    }
    return lowercase.windowed(3, 1)
        .distinct()
}

fun BitSet.getAllSetBitPositions(): List<Int> {
    val setBitPositions = mutableListOf<Int>()
    var i = this.nextSetBit(0)
    while (i >= 0) {
        setBitPositions.add(i)
        i = this.nextSetBit(i + 1)
    }
    return setBitPositions
}
