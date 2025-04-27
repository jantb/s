import java.util.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.math.min

/**
 * Object pool for reusing Entry objects to reduce allocations
 */
private class EntryPool<T : Comparable<T>>(private val maxSize: Int = 64) {
    private val pool = ArrayDeque<Entry<T>>(maxSize)

    fun obtain(iterator: Iterator<T>, value: T): Entry<T> {
        return if (pool.isNotEmpty()) {
            pool.removeFirst().apply {
                this.iterator = iterator
                this.value = value
            }
        } else {
            Entry(iterator, value)
        }
    }

    fun recycle(entry: Entry<T>) {
        if (pool.size < maxSize) {
            pool.addLast(entry)
        }
    }

    // Thread-local pool for better concurrent performance
    companion object {
        private val threadLocal = ThreadLocal.withInitial { EntryPool<Comparable<Any>>() }

        @Suppress("UNCHECKED_CAST")
        fun <T : Comparable<T>> get(): EntryPool<T> = threadLocal.get() as EntryPool<T>
    }
}

/**
 * Mutable Entry class for reuse
 */
private class Entry<T : Comparable<T>>(
    var iterator: Iterator<T>,
    var value: T
)

/**
 * K-way merge for sorted sequences with improved performance
 * - Uses binary heap for efficient merging
 * - Object pooling for reduced allocations
 * - Direct-compare optimization for common case of 2 sequences
 */
fun <T : Comparable<T>> List<Sequence<T>>.merge(descending: Boolean = false): Sequence<T> = sequence {
    when (size) {
        0 -> return@sequence
        1 -> yieldAll(this@merge[0])
        2 -> {
            // Optimize common case of merging 2 sequences
            mergeTwoSequences(this@merge[0], this@merge[1], descending).forEach {
                yield(it)
            }
        }
        else -> {
            // Optimize for small number of sequences with direct merging
            if (size <= 8) {
                mergeWithDirectCompare(descending).forEach {
                    yield(it)
                }
            } else {
                // For many sequences, use the binary heap approach
                mergeWithHeap(descending).forEach {
                    yield(it)
                }
            }
        }
    }
}

/**
 * Optimized merge for exactly two sequences
 */
private fun <T : Comparable<T>> mergeTwoSequences(
    seq1: Sequence<T>,
    seq2: Sequence<T>,
    descending: Boolean
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

    // Get first values
    var val1 = iter1.next()
    var val2 = iter2.next()

    // Merge with direct comparison (no heap overhead)
    while (true) {
        val compare = if (descending) val2.compareTo(val1) else val1.compareTo(val2)

        if (compare <= 0) {
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
 * Optimized merge for small number of sequences (≤8)
 * Uses direct comparison instead of heap operations
 */
private fun <T : Comparable<T>> List<Sequence<T>>.mergeWithDirectCompare(
    descending: Boolean
): Sequence<T> = sequence {
    val iterators = ArrayList<Iterator<T>>(size)
    val currentValues = ArrayList<T?>(size)

    // Initialize with first values
    this@mergeWithDirectCompare.forEach { seq ->
        val iterator = seq.iterator()
        iterators.add(iterator)
        currentValues.add(if (iterator.hasNext()) iterator.next() else null)
    }

    // Count of remaining active iterators
    var remaining = iterators.count { it.hasNext() || currentValues[iterators.indexOf(it)] != null }

    // Merge while we have values
    while (remaining > 0) {
        // Find min/max value
        var bestIndex = -1
        var bestValue: T? = null

        for (i in currentValues.indices) {
            val value = currentValues[i] ?: continue

            if (bestValue == null) {
                bestIndex = i
                bestValue = value
            } else {
                val compare = if (descending) value.compareTo(bestValue) else bestValue.compareTo(value)
                if (compare > 0) {
                    bestIndex = i
                    bestValue = value
                }
            }
        }

        // Yield the found value
        bestValue?.let { yield(it) }

        // Replace consumed value
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
 * Traditional heap-based merge for larger numbers of sequences
 * Uses object pooling to reduce allocations
 */
private fun <T : Comparable<T>> List<Sequence<T>>.mergeWithHeap(
    descending: Boolean
): Sequence<T> = sequence {
    val pool = EntryPool.get<T>()
    val comparator = if (descending)
        compareByDescending<Entry<T>> { it.value }
    else
        compareBy { it.value }

    // Use a more efficient binary heap implementation
    val heap = PriorityQueue(min(size + 1, 1024), comparator)

    // Initialize heap with first values
    for (sequence in this@mergeWithHeap) {
        val iterator = sequence.iterator()
        if (iterator.hasNext()) {
            val value = iterator.next()
            heap.add(pool.obtain(iterator, value))
        }
    }

    // Process the heap
    while (heap.isNotEmpty()) {
        val entry = heap.poll()
        yield(entry.value)

        // Get next value from this iterator if available
        if (entry.iterator.hasNext()) {
            val nextValue = entry.iterator.next()
            entry.value = nextValue
            heap.add(entry)
        } else {
            pool.recycle(entry)
        }
    }
}

/**
 * Extension function for suspension support with buffer optimization
 */
suspend fun <T : Comparable<T>> List<Sequence<T>>.mergeAsync(descending: Boolean = false): Flow<T> = flow {
    // Use a buffered implementation to improve throughput
    withContext(Dispatchers.Default) {
        val merged = merge(descending)

        // Process in chunks for better performance
        val buffer = ArrayList<T>(1024)
        merged.forEach { value ->
            buffer.add(value)

            if (buffer.size >= 1024) {
                buffer.forEach { emit(it) }
                buffer.clear()
            }
        }

        // Emit any remaining items
        buffer.forEach { emit(it) }
    }
}