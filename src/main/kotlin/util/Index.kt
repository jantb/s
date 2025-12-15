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
        require(!isHigherRank) { "Can not add values to a higher rank index" }
        if (s.isBlank()) return

        val grams = s.grams()
        val shardSize = Integer.numberOfTrailingZeros(estimate(grams.size, probability))

        shardArray[shardSize]?.add(grams, t) ?: run {
            shardArray[shardSize] = Shard<T>(1 shl shardSize).apply { add(grams, t) }
        }
        size++
    }

    fun searchMustInclude(valueListList: List<List<String>>, f: (T) -> Boolean): Sequence<T> {
        // Must include all the strings in each of the lists
        val gramsList = valueListList.map { stringList ->
            buildList {
                for (str in stringList) addAll(str.grams())
            }
        }

        val result =
            shardArray.mapNotNull { it?.search(gramsList)?.filter { v -> f(v) } }.merge()

        return result.sortedDescending()
    }

    fun convertToHigherRank() {
        require(!isHigherRank) { "Can not convert an already converted higher rank" }
        isHigherRank = true
        shardArray.forEach { it?.convertToHigherRankRows(goalCardinality) }
    }

    /**
     * Merge where `this` is already higher-rank and `other` is NOT higher-rank.
     * The merged index stays higher-rank; rows from `other` are folded to match ranks from `this`.
     */
    fun unionHigherRankWithLowerRank(other: Index<T>): Index<T> {
        require(this.isHigherRank && !other.isHigherRank) {
            "Expected: this index higher-rank, other index not higher-rank"
        }

        val out = Index<T>(probability = this.probability, goalCardinality = this.goalCardinality)
        out.isHigherRank = true
        out.size = this.size + other.size

        for (i in shardArray.indices) {
            val a = this.shardArray[i]
            val b = other.shardArray[i]

            out.shardArray[i] = when {
                a == null && b == null -> null
                a != null && b == null -> a.copyStandalone() // already higher rank
                a == null && b != null -> {
                    // No rank template to match; just convert b normally
                    b.copyStandalone().also { it.convertToHigherRankRows(goalCardinality) }
                }
                else -> a!!.unionHigherRankWithLowerRank(b!!)
            }
        }

        return out
    }

    /**
     * UNION two indexes *after* both have been converted to higher rank.
     *
     * The returned index will produce the same results as:
     *   this.search(...) UNION other.search(...)
     * (i.e. concatenated/merged results; duplicates kept if the same T is present in both).
     *
     * How it works:
     * - For each shard, we concatenate the two valueLists (A then B).
     * - For each row, we "materialize" both higher-rank rows back to rank 0 using reduceRank()
     *   (this does NOT recover lost info; it expands the higher-rank semantics exactly),
     *   then OR them together with a bit shift for B.
     *
     * This does NOT re-run convertToHigherRank() on the merged index (to avoid additional loss).
     */
    fun unionAfterHigherRank(other: Index<T>): Index<T> {
        require(this.isHigherRank && other.isHigherRank) {
            "Both indexes must already be converted to higher rank"
        }

        val out = Index<T>(probability = this.probability, goalCardinality = this.goalCardinality)
        out.isHigherRank = true
        out.size = this.size + other.size

        for (i in shardArray.indices) {
            val a = this.shardArray[i]
            val b = other.shardArray[i]

            out.shardArray[i] = when {
                a == null && b == null -> null
                a != null && b == null -> a.copyStandalone()
                a == null && b != null -> b.copyStandalone()
                else -> a!!.unionAfterHigherRank(b!!)
            }
        }
        return out
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
        gramList.forEach { g -> bf.addHash(g) }
        bf.getSetPositions().forEach { rows[it].setBit(valueList.size) }
        valueList.add(key)
    }

    /**
     * Merge where `this` is higher-rank and `other` is NOT higher-rank.
     * Result keeps higher-rank mode and matches the per-row ranks of `this`.
     *
     * Semantics: union by concatenating valueLists (A then B).
     */
    fun unionHigherRankWithLowerRank(other: Shard<T>): Shard<T> {
        require(this.isHigherRank && !other.isHigherRank) {
            "Expected: this shard higher-rank, other shard rank-0"
        }
        require(this.rows.size == other.rows.size) { "Shard m mismatch" }

        val m = rows.size
        val out = Shard<T>(m)
        out.isHigherRank = true

        val leftCount = this.valueList.size
        val rightCount = other.valueList.size

        out.valueList.addAll(this.valueList)
        out.valueList.addAll(other.valueList)

        val totalCount = leftCount + rightCount
        val neededWords = ((totalCount + 63) ushr 6).coerceAtLeast(1).nextPowerOf2()

        fun orShifted(dst: LongArray, src: LongArray, srcWordsInUse: Int, shiftBits: Int) {
            val wordShift = shiftBits ushr 6
            val bitShift = shiftBits and 63

            for (i in 0 until srcWordsInUse) {
                val v = src[i]
                val di = i + wordShift
                if (di >= dst.size) break

                if (bitShift == 0) {
                    dst[di] = dst[di] or v
                } else {
                    dst[di] = dst[di] or (v shl bitShift)
                    if (di + 1 < dst.size) {
                        dst[di + 1] = dst[di + 1] or (v ushr (64 - bitShift))
                    }
                }
            }
        }

        val maxPossibleRank = Integer.numberOfTrailingZeros(neededWords) // since neededWords is pow2

        for (i in 0 until m) {
            val targetRank = min(this.rows[i].rank, maxPossibleRank)

            // Left side: expand higher-rank semantics to rank0 BUT cap to original leftCount
            val a0 = this.rows[i].materializeToRank0Capped(leftCount)

            // Right side: ensure no garbage beyond rightCount (safe even if none)
            val b0 = other.rows[i].copyRow().also { it.clearFrom(rightCount) } // rank is already 0

            val merged = LongArray(neededWords)

            // OR in A (no shift)
            for (w in 0 until a0.wordsInUse) merged[w] = merged[w] or a0.words[w]

            // OR in B shifted by leftCount
            orShifted(merged, b0.words, b0.wordsInUse, leftCount)

            val outRow = Row(merged)
            outRow.rank = 0
            outRow.recomputeWordsInUse()

            // Compress to match this-row rank
            while (outRow.rank < targetRank && outRow.words.size > 1) {
                outRow.increaseRank()
            }

            out.rows[i] = outRow
        }

        return out
    }

    fun convertToHigherRankRows(goalCardinality: Double) {
        require(!isHigherRank)
        isHigherRank = true

        val neededWords = ((valueList.size + 63) ushr 6).coerceAtLeast(1).nextPowerOf2()

        rows.forEach { row ->
            if (row.words.size < neededWords) {
                row.words = row.words.copyOf(neededWords)
            }
        }

        rows.forEach { row ->
            if (row.words.calculateDensity() > 0.8) {
                while (row.words.size > 1) row.increaseRank()
            }
            while (row.words.calculateDensity() < goalCardinality && row.words.size > 1) {
                row.increaseRank()
            }
        }
    }

    fun search(gramsList: List<List<Int>>): Sequence<T> {
        if (gramsList.all { it.isEmpty() }) {
            return valueList.asReversed().asSequence()
        }

        val rowList = buildList {
            for (g in gramsList) {
                if (g.isNotEmpty()) addAll(getRows(g))
            }
        }

        val bitPositions = if (isHigherRank) rowList.sortedByDescending { it.rank } else rowList
        return getSetValues(bitPositions)
    }

    private fun getRows(grams: List<Int>): List<Row> {
        bf.clear()
        grams.forEach { bf.addHash(it) }
        return bf.getSetPositions().map { rows[it] }
    }

    private fun getSetValues(bitPositions: List<Row>): Sequence<T> {
        return if (isHigherRank) {
            val row = andRowsOfNotEqualLength(bitPositions)
            row.getAllSetBitPositionsRanked().mapNotNull { pos ->
                if (pos < valueList.size) valueList[pos] else null
            }
        } else {
            andRowsOfEqualLength(bitPositions).getAllSetBitPositions().map { valueList[it] }
        }
    }

    private fun andRowsOfNotEqualLength(rowsArray: List<Row>): Row {
        val res = Row(rowsArray[0].words.clone())
        res.rank = rowsArray[0].rank
        res.wordsInUse = rowsArray[0].wordsInUse

        for (i in 1..<rowsArray.size) {
            val row = rowsArray[i]
            while (res.rank != row.rank) {
                res.reduceRank()
            }
            res.and(row)
            if (res.isEmpty()) return res
        }
        return res
    }

    private fun andRowsOfEqualLength(rowsArray: List<Row>): Row {
        val first = rowsArray[0]
        val res = Row(first.words.copyOf(first.wordsInUse))
        res.rank = first.rank
        res.wordsInUse = first.wordsInUse

        for (i in 1..<rowsArray.size) {
            res.and(rowsArray[i])
            if (res.isEmpty()) return res
        }
        return res
    }

    fun copyStandalone(): Shard<T> {
        val out = Shard<T>(rows.size)
        out.isHigherRank = this.isHigherRank
        out.valueList.addAll(this.valueList)
        for (i in rows.indices) out.rows[i] = this.rows[i].copyRow()
        return out
    }

    /**
     * UNION two shards AFTER higher rank.
     * Result shard remains in "higher rank mode" (isHigherRank=true) but all rows are materialized to rank 0,
     * so results match exactly the union of both shards' search results.
     */
    fun unionAfterHigherRank(other: Shard<T>): Shard<T> {
        require(this.isHigherRank && other.isHigherRank) {
            "Both shards must already be converted to higher rank"
        }
        require(this.rows.size == other.rows.size) { "Shard m mismatch" }

        val m = rows.size
        val out = Shard<T>(m)
        out.isHigherRank = true

        // concatenate values (A then B)
        val leftCount = this.valueList.size
        out.valueList.addAll(this.valueList)
        out.valueList.addAll(other.valueList)

        val totalCount = out.valueList.size
        val neededWords = ((totalCount + 63) ushr 6).coerceAtLeast(1).nextPowerOf2()

        fun materializeRank0(r: Row): Row {
            val c = r.copyRow()
            while (c.rank > 0) c.reduceRank()
            c.recomputeWordsInUse()
            return c
        }

        fun orShifted(dst: LongArray, src: LongArray, srcWordsInUse: Int, shiftBits: Int) {
            val wordShift = shiftBits ushr 6
            val bitShift = shiftBits and 63

            for (i in 0 until srcWordsInUse) {
                val v = src[i]
                val di = i + wordShift
                if (di >= dst.size) break

                if (bitShift == 0) {
                    dst[di] = dst[di] or v
                } else {
                    dst[di] = dst[di] or (v shl bitShift)
                    if (di + 1 < dst.size) {
                        dst[di + 1] = dst[di + 1] or (v ushr (64 - bitShift))
                    }
                }
            }
        }

        for (i in 0 until m) {
            val a0 = materializeRank0(this.rows[i])
            val b0 = materializeRank0(other.rows[i])

            val merged = LongArray(neededWords)

            // OR in A
            for (w in 0 until a0.wordsInUse) merged[w] = merged[w] or a0.words[w]

            // OR in B shifted by leftCount
            orShifted(merged, b0.words, b0.wordsInUse, leftCount)

            val outRow = Row(merged)
            outRow.rank = 0
            outRow.recomputeWordsInUse()
            out.rows[i] = outRow
        }

        return out
    }
}

class Bf(m: Int) : Serializable {
    private val modMask = m - 1
    private val bitSet = BitSet(m)
    private var cardinality = 0

    fun addHash(l: Int) {
        val pos = (l and modMask)
        if (!bitSet.get(pos)) cardinality++
        bitSet.set(pos)
    }

    fun getSetPositions(): IntArray {
        val setBitPositions = IntArray(cardinality)
        setBitPositions.indices.forEach {
            setBitPositions[it] =
                if (it == 0) bitSet.nextSetBit(0)
                else bitSet.nextSetBit(setBitPositions[it - 1] + 1)
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

    fun copyRow(): Row = Row(words.clone()).also {
        it.wordsInUse = this.wordsInUse
        it.rank = this.rank
    }

    fun recomputeWordsInUse() {
        var i = words.size
        while (i > 0 && words[i - 1] == 0L) i--
        wordsInUse = i
    }

    fun clearFrom(bitIndex: Int) {
        if (bitIndex <= 0) {
            // clear everything
            java.util.Arrays.fill(words, 0L)
            wordsInUse = 0
            return
        }

        val wordIndex = bitIndex ushr 6
        if (wordIndex >= words.size) return

        val bitInWord = bitIndex and 63
        val mask = if (bitInWord == 0) 0L else (1L shl bitInWord) - 1L

        // keep only bits below bitIndex in that word
        words[wordIndex] = words[wordIndex] and mask

        // clear all words after
        for (i in wordIndex + 1 until words.size) words[i] = 0L

        // recompute wordsInUse
        recomputeWordsInUse()
    }

    fun materializeToRank0Capped(validBits: Int): Row {
        val c = copyRow()
        while (c.rank > 0) c.reduceRank()     // expand higher-rank semantics to rank-0 bitmap
        c.clearFrom(validBits)               // IMPORTANT: prevent repetition beyond validBits
        return c
    }

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
            if (newArray.calculateDensity() >= targetDensity) break
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

    fun isEmpty(): Boolean = wordsInUse == 0

    private fun recalculateWordsInUseFrom(index: Int) {
        val w = words
        var i = if (index > w.size) w.size else index
        if (i <= 0) {
            wordsInUse = 0
            return
        }

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
            yield(i)
            i = nextSetBit(i + 1)
        }
    }

    fun getAllSetBitPositionsRanked(): Sequence<Int> = sequence {
        val repeats = 1 shl rank
        val block = words.size shl 6

        var tmp = IntArray(32)
        var cnt = 0

        for (u in 0 until wordsInUse) {
            var w = words[u]
            while (w != 0L) {
                val bit = java.lang.Long.numberOfTrailingZeros(w)
                if (cnt == tmp.size) tmp = tmp.copyOf(tmp.size shl 1)
                tmp[cnt++] = (u shl 6) + bit
                w = w and (w - 1)
            }
        }

        for (i in 0 until repeats) {
            val offset = block * i
            for (j in 0 until cnt) yield(tmp[j] + offset)
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

fun LongArray.calculateDensity() =
    if (isEmpty()) 0.0 else sumOf { java.lang.Long.bitCount(it) }.toDouble() / (size * java.lang.Long.SIZE)

fun String.grams(): List<Int> = GramHasher.grams(this)

object GramHasher {
    private val builder = StringBuilder(16)
    private var hashArray = IntArray(8)
    private val singleHash = IntArray(1)
    private val singleHashList = singleHash.asList()

    fun grams(input: String): List<Int> {
        builder.clear()

        for (c in input) {
            if (c.isLetterOrDigit()) {
                val lc = when {
                    c in 'A'..'Z' -> (c.code or 0x20).toChar()
                    else -> c
                }
                builder.append(lc)
            }
        }

        val validCount = builder.length

        if (validCount < 3) {
            if (validCount == 0) return emptyList()

            var hash = 0x811c9dc5.toInt()
            for (i in 0 until validCount) {
                hash = hash xor builder[i].code
                hash *= 0x01000193
            }
            singleHash[0] = hash
            return singleHashList
        }

        val trigramCount = validCount - 2
        if (hashArray.size < trigramCount) {
            hashArray = IntArray(trigramCount.coerceAtLeast(hashArray.size * 2))
        }

        for (i in 0 until trigramCount) {
            var hash = 0x811c9dc5.toInt()
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