import java.util.*

fun <T> List<Sequence<Comparable<T>>>.merge(descending: Boolean = false): Sequence<Comparable<T>> = sequence {
    data class Entry(val iterator: Iterator<Comparable<T>>, val value: Comparable<T>)

    val priorityQueue = PriorityQueue<Entry> { e1, e2 ->
        @Suppress("UNCHECKED_CAST")
        if (descending) e2.value.compareTo(e1.value as T) else e1.value.compareTo(e2.value as T)
    }
    this@merge.forEach {
        it.iterator().let { iterator -> if (iterator.hasNext()) priorityQueue.add(Entry(iterator, iterator.next())) }
    }

    while (priorityQueue.isNotEmpty()) {
        val entry = priorityQueue.poll()
        yield(entry.value)
        if (entry.iterator.hasNext()) {
            priorityQueue.add(Entry(entry.iterator, entry.iterator.next()))
        }
    }
}
