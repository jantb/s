import java.util.*
import kotlin.math.min

/**
 * K-way merge for sorted sequences optimized for descending order only
 * - Uses direct comparison for 2 sequences
 * - Uses direct comparison for up to 8 sequences
 * - Uses a max-heap for more than 8 sequences
 */
fun <T : Comparable<T>> List<Sequence<T>>.merge(): Sequence<T> = sequence {
    when (size) {
        0 -> return@sequence
        1 -> yieldAll(this@merge[0])
        2 -> mergeTwoSequences(this@merge[0], this@merge[1]).forEach { yield(it) }
        else -> {
            if (size <= 8) {
                mergeWithDirectCompare().forEach { yield(it) }
            } else {
                mergeWithHeap().forEach { yield(it) }
            }
        }
    }
}

/**
 * Optimized merge for exactly two sequences (descending order)
 */
private fun <T : Comparable<T>> mergeTwoSequences(
    seq1: Sequence<T>,
    seq2: Sequence<T>
): Sequence<T> = sequence {
    val iter1 = seq1.iterator()
    val iter2 = seq2.iterator()

    // Short-circuit if either sequence is empty
    if (!iter1.hasNext()) {
        yieldAll(iter2)
        return@sequence
    }
    if (!iter2.hasNext()) {
        yieldAll(iter1)
        return@sequence
    }

    var val1 = iter1.next()
    var val2 = iter2.next()

    while (true) {
        if (val1.compareTo(val2) >= 0) {
            yield(val1)
            if (iter1.hasNext()) {
                val1 = iter1.next()
            } else {
                yield(val2)
                yieldAll(iter2)
                break
            }
        } else {
            yield(val2)
            if (iter2.hasNext()) {
                val2 = iter2.next()
            } else {
                yield(val1)
                yieldAll(iter1)
                break
            }
        }
    }
}

/**
 * Optimized merge for small number of sequences (≤8) using direct comparison (descending order)
 */
private fun <T : Comparable<T>> List<Sequence<T>>.mergeWithDirectCompare(): Sequence<T> = sequence {
    val iterators = ArrayList<Iterator<T>>(size)
    val currentValues = ArrayList<T?>(size)

    // Initialize with first values
    for (seq in this@mergeWithDirectCompare) {
        val iterator = seq.iterator()
        iterators.add(iterator)
        currentValues.add(if (iterator.hasNext()) iterator.next() else null)
    }

    var remaining = currentValues.count { it != null }

    // Merge while we have values
    while (remaining > 0) {
        var bestIndex = -1
        var bestValue: T? = null

        for (i in currentValues.indices) {
            val value = currentValues[i]
            if (value != null && (bestValue == null || value.compareTo(bestValue) > 0)) {
                bestIndex = i
                bestValue = value
            }
        }

        bestValue?.let { yield(it) }

        val iterator = iterators[bestIndex]
        if (iterator.hasNext()) {
            currentValues[bestIndex] = iterator.next()
        } else {
            currentValues[bestIndex] = null
            remaining--
        }
    }
}

/**
 * Heap-based merge for larger numbers of sequences (descending order)
 */
private fun <T : Comparable<T>> List<Sequence<T>>.mergeWithHeap(): Sequence<T> = sequence {
    val comparator = compareByDescending<Entry<T>> { it.value }
    val heap = PriorityQueue(min(size + 1, 1024), comparator)

    // Initialize heap with first values
    for (sequence in this@mergeWithHeap) {
        val iterator = sequence.iterator()
        if (iterator.hasNext()) {
            val value = iterator.next()
            heap.add(Entry(iterator, value))
        }
    }

    // Process the heap
    while (heap.isNotEmpty()) {
        val entry = heap.poll()
        yield(entry.value)

        // Get next value from this iterator if available
        if (entry.iterator.hasNext()) {
            val nextValue = entry.iterator.next()
            heap.add(Entry(entry.iterator, nextValue))
        }
    }
}

/**
 * Simple Entry class for heap-based merge
 */
private class Entry<T>(val iterator: Iterator<T>, val value: T)