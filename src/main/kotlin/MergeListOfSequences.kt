fun <T : Comparable<T>> List<Sequence<T>>.merge(): Sequence<T> = sequence {
    if (isEmpty()) return@sequence

    // Initialize iterators and current values
    val iterators = map { it.iterator() }
    val currentValues = MutableList(size) { i -> iterators[i].takeIf { it.hasNext() }?.next() }

    // Continue while any sequence has values
    while (currentValues.any { it != null }) {
        // Find the index of the maximum value
        var maxIndex = -1
        var maxValue: T? = null

        for (i in currentValues.indices) {
            val value = currentValues[i]
            if (value != null && (maxValue == null || value > maxValue)) {
                maxIndex = i
                maxValue = value
            }
        }

        // Yield the maximum value if found
        if (maxValue != null) {
            yield(maxValue)

            // Advance the iterator for the selected sequence
            val iter = iterators[maxIndex]
            currentValues[maxIndex] = if (iter.hasNext()) iter.next() else null
        }
    }
}