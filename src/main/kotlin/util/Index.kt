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

        val shardSize = Integer.numberOfTrailingZeros(estimate(grams.a.size, probability))
        shardArray[shardSize]?.add(grams, t) ?: run {
            shardArray[shardSize] =
                Shard<T>(1 shl shardSize).apply {
                    add(grams, t)
                }
        }
        size++
    }

    fun searchMustInclude(valueListList: List<List<String>>, f: (T) -> Boolean): Sequence<T> {
        // Build one primitive gram array per inner list
        val gramsList: List<IntArray> = valueListList.map { stringList ->
            var buf = IntArray(16)
            var n = 0

            fun ensure(extra: Int) {
                val need = n + extra
                if (need > buf.size) {
                    var newSize = buf.size
                    while (newSize < need) newSize = newSize shl 1
                    buf = buf.copyOf(newSize)
                }
            }

            for (str in stringList) {
                if (str.isBlank()) continue
                val g = str.grams()          // Grams(val a: IntArray, val n: Int)
                if (g.n == 0) continue
                ensure(g.n)
                g.a.copyInto(buf, destinationOffset = n, startIndex = 0, endIndex = g.n)
                n += g.n
            }

            buf.copyOf(n)
        }

        val result =
            shardArray
                .mapNotNull { it?.search(gramsList)?.filter(f) }
                .merge()

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
    fun add(gramList: Grams, key: T) {
        bf.clear()
        val a = gramList.a
        for (i in 0 until gramList.n) bf.addHash(a[i])
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

    fun search(gramsList: List<IntArray>): Sequence<T> {
        if (gramsList.all { it.isEmpty() }) {
            return valueList.asReversed().asSequence()
        }

        val rowList = buildList {
            for (g in gramsList) {
                if (g.isNotEmpty()) addAll(getRows(g))
            }
        }

        val bitPositions = if (isHigherRank) {
            rowList.sortedByDescending { it.rank }
        } else {
            rowList
        }

        return getSetValues(bitPositions)
    }

    private fun getRows(
        grams: IntArray,
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
            row.getAllSetBitPositionsRanked().mapNotNull {
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

    private fun andRowsOfEqualLength(rowsArray: List<Row>): Row {
        val first = rowsArray[0]
        val res = Row(first.words.copyOf(first.wordsInUse))  // copy only used words
        res.rank = first.rank
        res.wordsInUse = first.wordsInUse

        for (i in 1..<rowsArray.size) {
            res.and(rowsArray[i])
            if (res.isEmpty()) return res
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

        val wordIndex = bitToSet ushr 6
        words[wordIndex] = words[wordIndex] or (1L shl (bitToSet and 63))

        val needed = wordIndex + 1
        if (needed > wordsInUse) wordsInUse = needed
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
        val w = words
        var i = if (index > w.size) w.size else index
        if (i <= 0) {
            wordsInUse = 0
            return
        }

        // Fast path: last candidate is already non-zero
        if (w[i - 1] != 0L) {
            wordsInUse = i
            return
        }

        while (i > 0 && w[i - 1] == 0L) i--
        wordsInUse = i
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

    fun getAllSetBitPositionsRanked(): Sequence<Int> = sequence {
        val repeats = 1 shl rank
        val block = words.size shl 6 // words.size * 64

        // Collect base set bit positions once (in increasing order)
        var tmp = IntArray(32)
        var cnt = 0

        for (u in 0 until wordsInUse) {
            var w = words[u]
            while (w != 0L) {
                val bit = java.lang.Long.numberOfTrailingZeros(w)
                if (cnt == tmp.size) tmp = tmp.copyOf(tmp.size shl 1)
                tmp[cnt++] = (u shl 6) + bit
                w = w and (w - 1) // clear lowest set bit
            }
        }

        // Emit in the same order as before: by rank-block, then by bit position
        for (i in 0 until repeats) {
            val offset = block * i
            for (j in 0 until cnt) {
                yield(tmp[j] + offset)
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


data class Grams(val a: IntArray, val n: Int)

fun String.grams(): Grams = GramHasher.gramsArr(this)

object GramHasher {
    private val builder = StringBuilder(16)
    private var hashArray = IntArray(8)
    private val singleHash = IntArray(1)

    fun gramsArr(input: String): Grams {
        builder.clear()
        for (c in input) {
            if (c.isLetterOrDigit()) {
                val lc = if (c in 'A'..'Z') (c.code or 0x20).toChar() else c
                builder.append(lc)
            }
        }

        val validCount = builder.length
        if (validCount < 3) {
            if (validCount == 0) return Grams(IntArray(0), 0)
            var hash = 0x811c9dc5.toInt()
            for (i in 0 until validCount) {
                hash = (hash xor builder[i].code) * 0x01000193
            }
            singleHash[0] = hash
            return Grams(singleHash, 1)
        }

        val trigramCount = validCount - 2
        if (hashArray.size < trigramCount) {
            hashArray = IntArray(maxOf(trigramCount, hashArray.size * 2))
        }

        for (i in 0 until trigramCount) {
            var hash = 0x811c9dc5.toInt()
            hash = (hash xor builder[i].code) * 0x01000193
            hash = (hash xor builder[i + 1].code) * 0x01000193
            hash = (hash xor builder[i + 2].code) * 0x01000193
            hashArray[i] = hash
        }
        return Grams(hashArray, trigramCount)
    }
}