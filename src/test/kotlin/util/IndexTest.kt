package util

import org.junit.jupiter.api.Assertions.assertTrue
import printBytesAsAppropriateUnit
import serializeToBytes
import serializeToFile
import java.util.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.measureTime


class IndexTest {
    @Test
    fun test_add_success() {
        val index = Index<String>()
        val key = "key"
        val value = "value"

        index.add(key, value)

        assertEquals(1, index.size)
    }

    @Test
    fun test_search_success() {
        val index = Index<String>()
        val key = "key"
        val value = "value"

        index.add(key, value)

        assertEquals(1, index.size)

        val searchMustInclude = index.searchMustInclude(listOf(listOf("val"))).toList()
        assertEquals(1, searchMustInclude.size)
    }

    @Test
    fun test_search_success_two_items() {
        val index = Index<String>()
        val key = "key"
        val key2 = "key2"
        val value = "a"
        val value2 = "b"

        index.add(key, value)
        index.add(key2, value2)

        assertEquals(2, index.size)

        val searchMustInclude = index.searchMustInclude(listOf(listOf("a"))).toList()
        assertEquals(1, searchMustInclude.size)
    }

    @Test
    fun regression_test() {
        val index = Index<Int>()
        val itemCount = 300_000

        val map = mutableMapOf<Int, String>() // For later verification

        val timeTaken = measureTime {
            for (key in 1..itemCount) {
                val value = UUID.randomUUID().toString()

                index.add(key, value)

                map[key] = value // Save information for verification
            }
        }
        println("Time taken to add $itemCount elements: $timeTaken")
        println(index.serializeToBytes().size.printBytesAsAppropriateUnit())
        var found = 0
        val timeTakenSearch = measureTime {
            for ((key, value) in map) {
                val searchMustInclude = index.searchMustInclude(listOf(listOf(value)))
                if (key in searchMustInclude) {
                    found++
                }
            }
        }
        assertEquals(itemCount, found)
        println("Time taken to search $itemCount elements: $timeTakenSearch, average ${(timeTakenSearch.inWholeNanoseconds / itemCount).nanoseconds}")
    }

    @Test
    fun regression_test_higher_rank() {
        val index = Index<Int>()
        val itemCount = 300_000

        val map = mutableMapOf<Int, String>() // For later verification

        val timeTaken = measureTime {
            for (key in 1..itemCount) {
                val value = UUID.randomUUID().toString()

                index.add(key, value)

                map[key] = value // Save information for verification
            }
        }
        index.convertToHigherRank()
        println(index.serializeToBytes().size.printBytesAsAppropriateUnit())
        println("Time taken to add $itemCount elements: $timeTaken")
        var found = 0
        val timeTakenSearch = measureTime {
            for ((key, value) in map) {
                val searchMustInclude = index.searchMustInclude(listOf(listOf(value)))
                if (key in searchMustInclude) {
                  found++
                }
            }
        }
        assertEquals(itemCount, found)
        println("Time taken to search $itemCount elements: $timeTakenSearch, average ${(timeTakenSearch.inWholeNanoseconds / itemCount).nanoseconds}")
    }
}

class ShardTest {
    @Test
    fun test_search() {
        val shard = Shard<String>(5)
        val gramList = listOf(12345L, 23456L)
        val key = "key"

        shard.add(gramList, key)
        val searchResults = shard.search(listOf(gramList)).toList()

        assertEquals(listOf(key), searchResults)
    }
}

class BitSetTest {

    private val row = Row()
    @Test
    fun `expandToFitBit does nothing when words size is sufficient`() {
        row.words = LongArray(10)
        val wordsInUseBefore = row.wordsInUse
        row.expandToFitBit(5)
        assertEquals(row.wordsInUse, wordsInUseBefore)
    }

    @Test
    fun `expandToFitBit expands words size when insufficient`() {
        row.words = LongArray(1)
        row.expandToFitBit(70)
        assertEquals(2, row.words.size)
    }

    @Test
    fun `expandToFitBit expands words size when insufficient, set 0`() {
        row.words = LongArray(0)
        row.expandToFitBit(0)
        assertEquals(1, row.words.size)
    }
    @Test
    fun `expandToFitBit expands words size when insufficient, set 1`() {
        row.words = LongArray(0)
        row.expandToFitBit(1)
        assertEquals(1, row.words.size)
    }

    @Test
    fun `expandToFitBit expands words size when insufficient, set 63`() {
        row.words = LongArray(0)
        row.expandToFitBit(63)
        assertEquals(1, row.words.size)
    }
    @Test
    fun `expandToFitBit expands words size when insufficient, set 64`() {
        row.words = LongArray(0)
        row.expandToFitBit(64)
        assertEquals(2, row.words.size)
    }
    @Test
    fun `expandToFitBit expands words size when insufficient, set 65`() {
        row.words = LongArray(0)
        row.expandToFitBit(65)
        assertEquals(2, row.words.size)
    }
}