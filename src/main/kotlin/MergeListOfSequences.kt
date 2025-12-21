import java.util.PriorityQueue

fun <T : Comparable<T>> Sequence<Sequence<T>>.merge(): Sequence<T> = sequence {
    val outer = iterator()
    if (!outer.hasNext()) return@sequence

    val iterators = mutableListOf<Iterator<T>>()
    val queue = PriorityQueue<Pair<T, Int>>(compareByDescending { it.first })

    fun addIterator(iter: Iterator<T>) {
        val index = iterators.size
        iterators += iter
        if (iter.hasNext()) queue.offer(iter.next() to index)
    }

    // Seed with all inner sequences that exist at start
    while (outer.hasNext()) {
        addIterator(outer.next().iterator())
    }

    while (queue.isNotEmpty()) {
        val (maxValue, sourceIndex) = queue.poll()
        yield(maxValue)

        val it = iterators[sourceIndex]
        if (it.hasNext()) {
            queue.offer(it.next() to sourceIndex)
        }
    }
}

fun <T : Comparable<T>> List<Sequence<T>>.merge(): Sequence<T> =
    asSequence().merge()
