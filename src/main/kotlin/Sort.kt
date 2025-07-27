import kotlinx.coroutines.*
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.log2

/**
 * Object pool for temporary arrays used during merging
 */
@Suppress("UNCHECKED_CAST")
class ArrayPool<T>(private val maxPoolSize: Int = 64) {
    private val pool = ConcurrentLinkedQueue<Array<Any?>>()

    @Suppress("UNCHECKED_CAST")
    fun obtain(size: Int): Array<T> {
        val array = pool.poll()?.takeIf { it.size >= size }
        return (array ?: arrayOfNulls<Any?>(size)) as Array<T>
    }

    fun recycle(array: Array<T>) {
        if (pool.size < maxPoolSize) {
            pool.offer(array as Array<Any?>)
        }
    }

    // Extension function to make it easier to use with the pool
    inline fun <R> useArray(size: Int, block: (Array<T>) -> R): R {
        val array = obtain(size)
        try {
            return block(array)
        } finally {
            recycle(array)
        }
    }
}

// Thread-local pools to avoid contention
private val sortingPools = ThreadLocal.withInitial { ArrayPool<Any?>() }

@Suppress("UNCHECKED_CAST")
private fun <T> getPool(): ArrayPool<T> = sortingPools.get() as ArrayPool<T>

// Limited dispatcher for sorting operations
private val sortingDispatcher by lazy {
    Dispatchers.Default.limitedParallelism(
        (Runtime.getRuntime().availableProcessors().coerceAtLeast(2))
    )
}

/**
 * Inline function to sort by descending order using the provided selector
 */
inline fun <T, R : Comparable<R>> Iterable<T>.sortedByDescending(crossinline selector: (T) -> R?): List<T> {
    return sortedWith(compareByDescending(selector))
}

/**
 * Extension functions for Sequence
 */
suspend fun <T : Comparable<T>> Sequence<T>.parallelSorted(): Sequence<T> {
    val list = this.toMutableList()
    parallelSortInPlace(list, naturalOrder())
    return list.asSequence()
}

suspend fun <T : Comparable<T>> Sequence<T>.parallelSortedDescending(): Sequence<T> {
    val list = this.toMutableList()
    parallelSortInPlace(list, reverseOrder())
    return list.asSequence()
}

suspend fun <T> Sequence<T>.parallelSortedWith(comparator: Comparator<T>): Sequence<T> {
    val list = this.toMutableList()
    parallelSortInPlace(list, comparator)
    return list.asSequence()
}

suspend fun <T> Sequence<T>.parallelSortedDescendingWith(comparator: Comparator<T>): Sequence<T> {
    val list = this.toMutableList()
    parallelSortInPlace(list, comparator.reversed())
    return list.asSequence()
}

/**
 * Extension functions for List
 */
suspend fun <T : Comparable<T>> MutableList<T>.parallelSort(): MutableList<T> {
    return parallelSortWith(naturalOrder())
}

suspend fun <T : Comparable<T>> MutableList<T>.parallelSortDescending(): MutableList<T> {
    return parallelSortWith(reverseOrder())
}

suspend fun <T> MutableList<T>.parallelSortWith(comparator: Comparator<T>): MutableList<T> {
    parallelSortInPlace(this, comparator)
    return this
}

suspend fun <T> MutableList<T>.parallelSortDescendingWith(comparator: Comparator<T>): MutableList<T> {
    parallelSortInPlace(this, comparator.reversed())
    return this
}


/**
 * Dynamically determine threshold based on list size
 * Optimized for 1000-2000 element lists
 */
private fun dynamicThreshold(size: Int): Int {
    return when {
        size > 5000 -> 1000
        size > 2000 -> 750
        size > 1000 -> 500
        size > 500 -> 250
        else -> 128
    }
}

/**
 * Calculate maximum depth based on available processors
 */
private fun availableProcessorsDepth(): Int {
    val cores = Runtime.getRuntime().availableProcessors()
    return cores.takeIf { it > 1 }?.let { log2(it.toDouble()).toInt() + 1 } ?: 1
}

/**
 * Insertion sort for small arrays - truly in-place
 */
private fun <T> insertionSort(list: MutableList<T>, fromIndex: Int, toIndex: Int, comparator: Comparator<T>) {
    for (i in fromIndex + 1 until toIndex) {
        val key = list[i]
        var j = i - 1
        while (j >= fromIndex && comparator.compare(list[j], key) > 0) {
            list[j + 1] = list[j]
            j--
        }
        list[j + 1] = key
    }
}

/**
 * Main parallel sort function that sorts in-place
 */
suspend fun <T> parallelSortInPlace(
    list: MutableList<T>,
    comparator: Comparator<T>,
    threshold: Int = dynamicThreshold(list.size),
    maxDepth: Int = availableProcessorsDepth()
): Unit = coroutineScope {
    parallelSortInPlaceRecursive(list, 0, list.size, comparator, 0, threshold, maxDepth)
}

/**
 * Recursive implementation of parallel merge sort that operates in-place
 */
private suspend fun <T> parallelSortInPlaceRecursive(
    list: MutableList<T>,
    fromIndex: Int,
    toIndex: Int,
    comparator: Comparator<T>,
    depth: Int,
    threshold: Int,
    maxDepth: Int
): Unit = coroutineScope {
    val size = toIndex - fromIndex

    // Base case: empty or single element
    if (size <= 1) return@coroutineScope

    // Use appropriate sort based on size
    if (size <= 16) {
        // For very small arrays, use insertion sort
        insertionSort(list, fromIndex, toIndex, comparator)
        return@coroutineScope
    } else if (size <= threshold) {
        // For small-medium arrays, use built-in sort
        list.subList(fromIndex, toIndex).sortWith(comparator)
        return@coroutineScope
    }

    // For larger arrays, divide and conquer
    val middle = fromIndex + size / 2

    // Determine if we should parallelize based on depth
    if (depth < maxDepth) {
        // Parallelize sorting of left and right halves
        val leftJob = async(sortingDispatcher) {
            parallelSortInPlaceRecursive(
                list, fromIndex, middle, comparator, depth + 1, threshold, maxDepth
            )
        }

        // Sort right half in this coroutine
        parallelSortInPlaceRecursive(
            list, middle, toIndex, comparator, depth + 1, threshold, maxDepth
        )

        // Wait for left half to complete
        leftJob.await()
    } else {
        // Sequential execution for deeper levels
        parallelSortInPlaceRecursive(
            list, fromIndex, middle, comparator, depth + 1, threshold, maxDepth
        )
        parallelSortInPlaceRecursive(
            list, middle, toIndex, comparator, depth + 1, threshold, maxDepth
        )
    }

    // Merge the two sorted halves in-place
    mergeInPlace(list, fromIndex, middle, toIndex, comparator)
}

/**
 * Merge two adjacent sorted ranges in-place
 * Uses a temporary array but only for the smaller half to minimize memory usage
 */
private fun <T> mergeInPlace(
    list: MutableList<T>,
    fromIndex: Int,
    middleIndex: Int,
    toIndex: Int,
    comparator: Comparator<T>
) {
    val leftSize = middleIndex - fromIndex
    val rightSize = toIndex - middleIndex

    // If one side is empty, no merge needed
    if (leftSize == 0 || rightSize == 0) return

    // Quick check if already sorted
    if (comparator.compare(list[middleIndex - 1], list[middleIndex]) <= 0) return

    // Use the smaller side as temporary to minimize memory usage
    if (leftSize <= rightSize) {
        mergeLeftIntoRight(list, fromIndex, middleIndex, toIndex, comparator)
    } else {
        mergeRightIntoLeft(list, fromIndex, middleIndex, toIndex, comparator)
    }
}

/**
 * Merge by copying left side to temp array (when left side is smaller)
 */
private fun <T> mergeLeftIntoRight(
    list: MutableList<T>,
    fromIndex: Int,
    middleIndex: Int,
    toIndex: Int,
    comparator: Comparator<T>
) {
    val leftSize = middleIndex - fromIndex

    // Get a temporary array from the pool
    getPool<T>().useArray(leftSize) { temp ->
        // Copy left side to temp array
        for (i in 0 until leftSize) {
            temp[i] = list[fromIndex + i]
        }

        // Merge back into original list
        var left = 0
        var right = middleIndex
        var dest = fromIndex

        while (left < leftSize && right < toIndex) {
            val leftVal = temp[left]
            val rightVal = list[right]

            if (comparator.compare(leftVal, rightVal) <= 0) {
                list[dest++] = leftVal
                left++
            } else {
                list[dest++] = rightVal
                right++
            }
        }

        // Copy any remaining elements from temp
        while (left < leftSize) {
            list[dest++] = temp[left++]
        }
        // No need to copy remaining right elements as they're already in place
    }
}

/**
 * Merge by copying right side to temp array (when right side is smaller)
 */
private fun <T> mergeRightIntoLeft(
    list: MutableList<T>,
    fromIndex: Int,
    middleIndex: Int,
    toIndex: Int,
    comparator: Comparator<T>
) {
    val rightSize = toIndex - middleIndex

    // Get a temporary array from the pool
    getPool<T>().useArray(rightSize) { temp ->
        // Copy right side to temp array
        for (i in 0 until rightSize) {
            temp[i] = list[middleIndex + i]
        }

        // Merge back into original list
        var left = middleIndex - 1  // Last element of left side
        var right = rightSize - 1   // Last element of right side in temp
        var dest = toIndex - 1      // Last position in result

        // Merge from back to front
        while (left >= fromIndex && right >= 0) {
            val leftVal = list[left]
            val rightVal = temp[right]

            if (comparator.compare(rightVal, leftVal) >= 0) {
                list[dest--] = rightVal
                right--
            } else {
                list[dest--] = leftVal
                left--
            }
        }

        // Copy any remaining elements from temp
        while (right >= 0) {
            list[dest--] = temp[right--]
        }
        // No need to copy remaining left elements as they're already in place
    }
}

/**
 * Extension to calculate log base 2 of a double
 */
private fun log2(value: Double): Double = kotlin.math.log(value, 2.0)