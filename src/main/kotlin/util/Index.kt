package util

import merge
import sortedByDescending
import util.Bf.Companion.estimate
import java.io.Serializable
import java.util.*
import kotlin.math.*

class Index<T : Comparable<T>>(
    private val probability: Double = 0.0001,
    private val goalCardinality: Double = 0.30,
) : Serializable {
    private var shardArray: Array<Shard<T>?> = Array(32) { null }
    private var isHigherRank: Boolean = false
    var size: Int = 0
    fun add(t: T, s: String) {
        require(!isHigherRank) {
            "Can not add values to a higher rank index"
        }
        if (s.contains("f14a5d77-8445-4e92-8a5d-7784457e9287")) {
            println()
        }
        if (s.isBlank()) {
            return
        }

        val grams = s.grams()

        val shardSize = Integer.numberOfTrailingZeros(estimate(grams.size, probability))
        shardArray[shardSize]?.add(grams, t) ?: run {
            shardArray[shardSize] =
                Shard<T>(1 shl shardSize).apply {
                    add(grams, t)
                }
        }
        size++
    }

    fun searchMustInclude(valueListList: List<List<String>>, f: (T) -> Boolean): Sequence<T> {
        // Must include all the strings in each of the lists
        val gramsList = valueListList.map { stringList -> stringList.map { it.grams() }.flatten() }
        val result =
            shardArray.mapNotNull { it?.search(gramsList)?.filter { f(it) } }.merge()

        return result.sortedDescending()
    }

    fun convertToHigherRank() {
        require(!isHigherRank) {
            "Can not convert an already converted higher rank"
        }
        isHigherRank = true
        shardArray.forEach { it?.convertToHigherRankRows(goalCardinality) }
    }
}

class Shard<T>(
    m: Int,
) : Serializable {
    private val bf = Bf(m)
    private val valueList: MutableList<T> = mutableListOf()
    private val rows: Array<Row> = Array(m) { Row() }
    private var isHigherRank: Boolean = false
    fun add(gramList: List<Int>, key: T) {
        bf.clear()
        gramList.forEach { g ->
            bf.addHash(g)
        }
        bf.getSetPositions().forEach {
            rows[it].setBit(valueList.size)
        }
        valueList.add(key)
    }

    fun convertToHigherRankRows(goalCardinality: Double) {
        require(!isHigherRank) {
            "Can not convert an already converted higher rank"
        }
        isHigherRank = true
        rows.forEach {

            if (it.words.calculateDensity() > 0.8) {
                while (it.words.size > 1) {
                    it.increaseRank()
                }
            }
            while (it.words.calculateDensity() < goalCardinality && it.words.size > 1) {
                it.increaseRank()
            }
        }
    }

    fun search(gramsList: List<List<Int>>): Sequence<T> {
        if (gramsList.flatten().isEmpty()) {
            return valueList.asReversed().asSequence()
        }

        val rowList = gramsList.filter { it.isNotEmpty() }.map { getRows(it) }.flatten()

        val bitPositions = if (isHigherRank) {
            rowList.sortedByDescending { it.rank }
        } else {
            rowList
        }

        return getSetValues(bitPositions)
    }

    private fun getRows(
        grams: List<Int>,
    ): List<Row> {
        bf.clear()
        grams.forEach {
            bf.addHash(it)
        }
        return bf.getSetPositions().map { rows[it] }
    }

    private fun getSetValues(bitPositions: List<Row>): Sequence<T> {
        return if (isHigherRank) {
            val row = andRowsOfNotEqualLength(bitPositions)
            return row.getAllSetBitPositionsRanked().mapNotNull {
                if (it < valueList.size)
                    valueList[it]
                else
                    null
            }
        } else {
            andRowsOfEqualLength(bitPositions).getAllSetBitPositions().map { valueList[it] }
        }
    }

    private fun andRowsOfNotEqualLength(
        rowsArray: List<Row>,
    ): Row {
        val res = Row(rowsArray[0].words.clone())
        res.rank = rowsArray[0].rank
        res.wordsInUse = rowsArray[0].wordsInUse

        for (i in 1..<rowsArray.size) {
            val row = rowsArray[i]
            while (res.rank != row.rank) {
                res.reduceRank()
            }
            res.and(row)
            if (res.isEmpty()) {
                return res
            }
        }

        return res
    }

    private fun andRowsOfEqualLength(
        rowsArray: List<Row>,
    ): Row {
        val res = Row(rowsArray[0].words.clone())
        res.rank = rowsArray[0].rank
        res.wordsInUse = rowsArray[0].wordsInUse

        for (i in 1..<rowsArray.size) {
            res.and(rowsArray[i])
            if (res.isEmpty()) {
                return res
            }
        }

        return res
    }
}

class Bf(m: Int) : Serializable {
    private val modMask = m - 1
    private val bitSet = BitSet(m)
    private var cardinality = 0
    fun addHash(l: Int) {
        val pos = (l and modMask)
        if (!bitSet.get(pos))
            cardinality++
        bitSet.set(pos)
    }

    fun getSetPositions(): IntArray {
        val setBitPositions = IntArray(cardinality)

        setBitPositions.indices.forEach {
            setBitPositions[it] = if (it == 0) bitSet.nextSetBit(0) else bitSet.nextSetBit(setBitPositions[it - 1] + 1)
        }
        return setBitPositions
    }

    fun clear() {
        cardinality = 0
        bitSet.clear()
    }

    companion object {
        fun estimate(n: Int, p: Double): Int {
            return (ceil((n * ln(p)) / ln(1.0 / 2.0.pow(ln(2.0)))).toInt() + 1).nextPowerOf2()
        }
    }
}

fun Int.nextPowerOf2(): Int {
    var n = this
    n--

    n = n or (n shr 1)
    n = n or (n shr 2)
    n = n or (n shr 4)
    n = n or (n shr 8)
    n = n or (n shr 16)

    return ++n
}

class Row(var words: LongArray = LongArray(0)) : Serializable {
    var wordsInUse = 0
    var rank = 0

    fun convertToTargetDensity(targetDensity: Double) {
        if (targetDensity < 0.0 || targetDensity > 1.0) {
            throw IllegalArgumentException("Target density should be between 0.0 and 1.0.")
        }

        while (words.size > 1) {
            val length = words.size
            val partSize = length / 2

            val newArray = LongArray(partSize) {
                words[it] or words[it + partSize]
            }

            words = newArray
            recalculateWordsInUseFrom(words.size)
            rank++
            if (newArray.calculateDensity() >= targetDensity) {
                break
            }
        }
    }

    fun increaseRank() {
        val newSize = words.size shr 1
        val firstPart = LongArray(newSize)
        val lastPart = LongArray(newSize)
        System.arraycopy(words, 0, firstPart, 0, newSize)
        System.arraycopy(words, newSize, lastPart, 0, newSize)

        for (i in 0 until newSize) {
            firstPart[i] = firstPart[i] or lastPart[i]
        }
        words = firstPart
        recalculateWordsInUseFrom(words.size)
        rank++
    }

    fun setBit(bitToSet: Int) {
        expandToFitBit(bitToSet)

        val originalIndex = bitToSet shr 6
        words[originalIndex] = words[originalIndex] or (1L shl (bitToSet and (java.lang.Long.SIZE - 1)))
        recalculateWordsInUseFrom(max(((bitToSet shr 6) + 1), wordsInUse))
    }

    fun expandToFitBit(bitToSet: Int) {
        require(bitToSet >= 0) { "bitToSet must be non-negative" }

        val neededSize = (bitToSet shr 6) + 1
        if (words.size < neededSize) {
            val newSize = maxOf(words.size * 2, neededSize)
            val powerOfTwoSize = newSize.nextPowerOf2()

            words = words.copyOf(powerOfTwoSize)
        }
    }

    fun reduceRank() {
        val newArray = LongArray(words.size * 2)
        System.arraycopy(words, 0, newArray, 0, words.size)
        System.arraycopy(words, 0, newArray, words.size, words.size)
        wordsInUse += words.size
        words = newArray
        rank--
    }

    fun isEmpty(): Boolean {
        return wordsInUse == 0
    }

    private fun recalculateWordsInUseFrom(index: Int) {
        // Traverse the bitset until a used word is found
        wordsInUse = index
        while (wordsInUse > 0) {
            if (words[wordsInUse - 1] != 0L) break
            wordsInUse--
        }
    }

    fun and(row: Row) {
        val min = min(wordsInUse, row.wordsInUse)

        for (i in 0..<min) words[i] = words[i] and row.words[i]

        recalculateWordsInUseFrom(min)
    }

    fun getAllSetBitPositions(): Sequence<Int> = sequence {
        var i = nextSetBit(0)
        while (i != -1) {
            yield(i) // Emits the value 'i' to the sequence and suspends until the next value is requested
            i = nextSetBit(i + 1)
        }
    }

    fun getAllSetBitPositionsRanked(): Sequence<Int> {
        val allSetBitPositions = getAllSetBitPositions()

        return sequence {
            for (i in 0 until (1 shl rank)) {
                allSetBitPositions.forEach {
                    yield(it + (words.size * 64 * i))
                }
            }
        }
    }

    fun nextSetBit(fromIndex: Int): Int {
        var u = fromIndex shr 6
        if (u >= wordsInUse) return -1
        var word = words[u] and (-0x1L shl fromIndex)
        while (true) {
            if (word != 0L) return (u shl 6) + java.lang.Long.numberOfTrailingZeros(word)
            if (++u == wordsInUse) return -1
            word = words[u]
        }
    }
}

fun LongArray.calculateDensity() = sumOf { java.lang.Long.bitCount(it) }.toDouble() / (size * java.lang.Long.SIZE)


fun String.grams(): List<Int> {
    return GramHasher.grams(this)
}


object GramHasher {
    private val builder = StringBuilder(16) // Initial capacity for small strings
    private var hashArray = IntArray(8) // Initial capacity for trigrams
    private val singleHash = IntArray(1) // Reusable single-element array
    private val singleHashList = singleHash.asList() // Cached List<Int> for short strings

    fun grams(input: String): List<Int> {
        builder.clear()

        // Build normalized string with valid characters only
        for (c in input) {
            if (c.isLetterOrDigit()) {
                val lc = when {
                    c in 'A'..'Z' -> (c.code or 0x20).toChar() // Bitwise lowercase
                    else -> c
                }
                builder.append(lc)
            }
        }

        val validCount = builder.length

        // Handle short strings (< 3 valid chars)
        if (validCount < 3) {
            if (validCount == 0) return emptyList()

            // Hash the entire short string
            var hash = 0x811c9dc5.toInt() // FNV-1a offset basis
            for (i in 0 until validCount) {
                hash = hash xor builder[i].code
                hash *= 0x01000193 // FNV-1a prime
            }
            singleHash[0] = hash
            return singleHashList
        }

        // For longer strings, compute trigrams using sliding window
        val trigramCount = validCount - 2
        if (hashArray.size < trigramCount) {
            hashArray = IntArray(trigramCount.coerceAtLeast(hashArray.size * 2))
        }

        // Sliding window FNV-1a hash
        for (i in 0 until trigramCount) {
            var hash = 0x811c9dc5.toInt() // FNV-1a offset basis
            hash = hash xor builder[i].code
            hash *= 0x01000193
            hash = hash xor builder[i + 1].code
            hash *= 0x01000193
            hash = hash xor builder[i + 2].code
            hash *= 0x01000193

            hashArray[i] = hash
        }
        return hashArray.asList().subList(0, trigramCount)
    }
}