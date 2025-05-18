import java.util.PriorityQueue

fun <T : Comparable<T>> List<Sequence<T>>.merge(): Sequence<T> = sequence {
    if (isEmpty()) return@sequence

    // Initialize iterators and priority queue
    val iterators = map { it.iterator() }.toTypedArray()
    val queue = PriorityQueue<Pair<T, Int>>(size, compareByDescending { it.first })

    // Initialize queue with first values
    iterators.forEachIndexed { i, iter ->
        if (iter.hasNext()) {
            queue.offer(iter.next() to i)
        }
    }

    // Continue while queue is not empty
    while (queue.isNotEmpty()) {
        // Get and yield the maximum value
        val (maxValue, maxIndex) = queue.poll()
        yield(maxValue)

        // Advance the iterator for the selected sequence
        if (iterators[maxIndex].hasNext()) {
            queue.offer(iterators[maxIndex].next() to maxIndex)
        }
    }
}