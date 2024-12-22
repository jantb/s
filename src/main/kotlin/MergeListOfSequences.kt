import java.util.*

fun <T: Comparable<T>> List<Sequence<T>>.merge(descending: Boolean = false): Sequence<T> = sequence {
    data class Entry(val iterator: Iterator<T>, val value: T)

    val priorityQueue = PriorityQueue<Entry> { e1, e2 ->
        if (descending) e2.value.compareTo(e1.value) else e1.value.compareTo(e2.value)
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
