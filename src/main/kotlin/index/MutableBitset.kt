package index

import kotlin.math.min

class MutableBitset(var words: LongArray = LongArray(0)) : Cloneable, java.io.Serializable {

    private val ADDRESS_BITS_PER_WORD = 6

    var wordsInUse = words.size
    fun set(bitIndex: Int) {
        if (bitIndex < 0) throw IndexOutOfBoundsException("bitIndex < 0: $bitIndex")
        val wordIndex = wordIndex(bitIndex)
        expandTo(wordIndex)
        words[wordIndex] = words[wordIndex] or (1L shl bitIndex)
    }

    operator fun get(bitIndex: Int): Boolean {
        if (bitIndex < 0) throw java.lang.IndexOutOfBoundsException("bitIndex < 0: $bitIndex")
        val wordIndex = wordIndex(bitIndex)
        return wordIndex < wordsInUse && words[wordIndex] and (1L shl bitIndex) != 0L
    }

    private fun wordIndex(bitIndex: Int): Int {
        return bitIndex shr ADDRESS_BITS_PER_WORD
    }

    private fun ensureCapacity(wordsRequired: Int) {
        if (words.size < wordsRequired) {
            // Allocate larger of doubled size or required size
            val request = (2 * words.size).coerceAtLeast(wordsRequired)
            words = words.copyOf(request)
        }
    }

    private fun expandTo(wordIndex: Int) {
        val wordsRequired = wordIndex + 1
        if (wordsInUse < wordsRequired) {
            ensureCapacity(wordsRequired)
            wordsInUse = wordsRequired
        }
    }

    fun and(set: MutableBitset) {
        if (this === set) return
        while (wordsInUse > set.wordsInUse) words[--wordsInUse] = 0

        for (i in 0 until wordsInUse) words[i] = words[i] and set.words[i]

        recalculateWordsInUse()
    }


    fun isEmpty(): Boolean = wordsInUse == 0

    fun clear() {
        wordsInUse = 0
    }

    private fun recalculateWordsInUse() {
        // Traverse the bitset until a used word is found
        var i: Int = wordsInUse - 1
        while (i >= 0) {
            if (words[i] != 0L) break
            i--
        }
        wordsInUse = i + 1 // The new logical size
    }

    fun or(set: MutableBitset) {
        if (this === set) return
        val wordsInCommon = min(wordsInUse, set.wordsInUse)
        if (wordsInUse < set.wordsInUse) {
            ensureCapacity(set.wordsInUse)
            wordsInUse = set.wordsInUse
        }

        // Perform logical OR on words in common
        for (i in 0 until wordsInCommon) words[i] = words[i] or set.words[i]

        // Copy any remaining words
        if (wordsInCommon < set.wordsInUse) System.arraycopy(
            set.words, wordsInCommon,
            words, wordsInCommon,
            wordsInUse - wordsInCommon
        )
    }

    public override fun clone(): MutableBitset {
        return try {
            val result = super.clone() as MutableBitset
            result.words = words.clone()
            result
        } catch (e: CloneNotSupportedException) {
            throw InternalError(e)
        }
    }
}

fun MutableBitset.appendCopy(size: Int) {
    val oldSize = size / 2
    if (size == this.words.size) {
        System.arraycopy(this.words, 0, this.words, oldSize, oldSize)
    } else {
        val newArray = LongArray(size)
        while (this.words.size < oldSize) {
            this.appendCopy(this.words.size * 2)
        }
        System.arraycopy(this.words, 0, newArray, 0, oldSize)
        System.arraycopy(this.words, 0, newArray, oldSize, oldSize)
        this.words = newArray
    }
    wordsInUse = size
}