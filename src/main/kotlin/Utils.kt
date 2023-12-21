import JsonMapper.objectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.SingletonSupport
import java.io.*
import java.text.DecimalFormat
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.roundToInt

fun Double.roundUp(value: Int): Int {
    return value * (ceil(abs(this / value))).toInt()
}

fun Int.round(value: Int): Int {
    if (value == 0) {
        return 0
    }
    return value * (abs(this.toDouble() / value).roundToInt())
}

fun Int.roundDown(value: Int): Int {
    return value * (floor(abs(this.toDouble() / value))).toInt()
}

inline fun <T> List<T>.forEachIndexedReverse(action: (index: Int, T) -> Unit) {
    for (index in size - 1 downTo 0) {
        action(index, this[index])
    }
}

fun <T> List<T>.asSequenceReversed(): Sequence<T> = sequence {
    for (index in size - 1 downTo 0) {
        yield(get(index))
    }
}

fun mergeSortDescending(lists: List<List<Int>>): Sequence<Int> {
    val activeIterators = lists.map { it.listIterator(it.size) }.toMutableList()
    val array = IntArray(activeIterators.size) { Int.MIN_VALUE }

    for ((index, iterator) in activeIterators.withIndex()) {
        if (iterator.hasPrevious()) {
            array[index] = iterator.previous()
        }
    }
    return sequence {
        while (activeIterators.isNotEmpty()) {
            var maxValue = Int.MIN_VALUE
            var maxIndex = -1

            for ((index, _) in activeIterators.withIndex()) {
                val currentValue = array[index]
                if (currentValue > maxValue) {
                    maxValue = currentValue
                    maxIndex = index
                }
            }

            if (maxIndex == -1) {
                break
            }
            yield(maxValue)

            val iterator = activeIterators[maxIndex]
            if (iterator.hasPrevious()) {
                array[maxIndex] = iterator.previous()
            } else {
                activeIterators.removeAt(maxIndex)
            }
        }
    }
}

fun Serializable.serializeToFile(filename: String) {
    FileOutputStream(filename).use { fileOutputStream ->
        ObjectOutputStream(fileOutputStream).use { objectOutputStream ->
            objectOutputStream.writeObject(this)
        }
    }
}

inline fun <reified T> deserializeFromFile(filename: String): T {
    return FileInputStream(filename).use { fileInputStream ->
        ObjectInputStream(fileInputStream).use { objectInputStream ->
            objectInputStream.readObject()
        }
    } as T
}

fun Int.format() = DecimalFormat("#,###").format(this)
    .replace(",", ".")

inline fun <reified T> String.deserializeJsonToObject(): T {
    return objectMapper.readValue(this, T::class.java)
}

inline fun <reified T> T.serializeToJson(): String {
    return objectMapper.writeValueAsString(this)
}
inline fun <reified T> T.serializeToJsonPP(): String {
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this)
}
fun mergeSequences(vararg sequences: Sequence<Int>): Sequence<Int> {
    fun mergeTwoSequences(sequence1: Sequence<Int>, sequence2: Sequence<Int>): Sequence<Int> {
        val iterator1 = sequence1.iterator()
        val iterator2 = sequence2.iterator()
        var element1: Int? = if (iterator1.hasNext()) iterator1.next() else null
        var element2: Int? = if (iterator2.hasNext()) iterator2.next() else null

        return sequence {
            while (element1 != null || element2 != null) {
                if (element1 != null && (element2 == null || element1!! >= element2!!)) {
                    yield(element1!!)
                    element1 = if (iterator1.hasNext()) iterator1.next() else null
                }
                else {
                    yield(element2!!)
                    element2 = if (iterator2.hasNext()) iterator2.next() else null
                }
            }
        }
    }

    if (sequences.size == 1) {
        return sequences[0]
    }

    val mid = sequences.size / 2
    val leftSequences = sequences.sliceArray(0 until mid)
    val rightSequences = sequences.sliceArray(mid until sequences.size)

    val mergedLeft = mergeSequences(*leftSequences)
    val mergedRight = mergeSequences(*rightSequences)

    return mergeTwoSequences(mergedLeft, mergedRight)
}

class LRUCache<T, V>(private val capacity: Int) : LinkedHashMap<T, V>(capacity, 0.75f, true) {
    override fun removeEldestEntry(eldest: MutableMap.MutableEntry<T, V>?): Boolean {
        return size > capacity
    }
}

object JsonMapper {
    val objectMapper: ObjectMapper = ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
        .registerModule(
        KotlinModule.Builder()
            .withReflectionCacheSize(512)
            .configure(KotlinFeature.NullToEmptyCollection, false)
            .configure(KotlinFeature.NullToEmptyMap, false)
            .configure(KotlinFeature.NullIsSameAsDefault, false)
            .configure(KotlinFeature.StrictNullChecks, false)
            .build()
    ).registerModule(JavaTimeModule())
}
