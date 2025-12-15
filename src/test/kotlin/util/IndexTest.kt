package util

import printBytesAsAppropriateUnit
import serializeToBytes
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

        index.add( value, value)

        assertEquals(1, index.size)
    }

    @Test
    fun test_search_success() {
        val index = Index<String>()
        val key = "key"
        val value = "value"

        index.add( value, value)

        assertEquals(1, index.size)

        val searchMustInclude = index.searchMustInclude(listOf(listOf("val"))) {
            true
        }.toList()
        assertEquals(1, searchMustInclude.size)
    }

    @Test
    fun test_search_success_two_items() {
        val index = Index<String>()
        val key = "key"
        val key2 = "key2"
        val value = "aaa"
        val value2 = "bbb"

        index.add( value, value)
        index.add( value2, value2)

        assertEquals(2, index.size)

        val searchMustInclude = index.searchMustInclude(listOf(listOf("aaa"))) {
            true
        }.toList()
        assertEquals(1, searchMustInclude.size)
    }

    @Test
    fun regression_test() {
        val index = Index<String>()
        val itemCount = 300_000

        val map = mutableSetOf< String>() // For later verification

        val timeTaken = measureTime {
            for (key in 1..itemCount) {
                val value = UUID.randomUUID().toString()

                index.add(value, value)

                map.add(value)
            }
        }
        println("Time taken to add $itemCount elements: $timeTaken")
        println(index.serializeToBytes().size.printBytesAsAppropriateUnit())
        var found = 0
        val timeTakenSearch = measureTime {
            for ( value in map) {
                val searchMustInclude = index.searchMustInclude(listOf(listOf(value))) {
                    it == value
                }
                if (value in searchMustInclude) {
                    found++
                }
            }
        }
        assertEquals(itemCount, found)
        println("Time taken to search $itemCount elements: $timeTakenSearch, average ${(timeTakenSearch.inWholeNanoseconds / itemCount).nanoseconds}")
    }

    @Test
    fun regression_test_higher_rank() {
        val index = Index<String        >()
        val itemCount = 500_000

        val map = mutableSetOf< String>() // For later verification

        val timeTaken = measureTime {
            for (key in 0..<itemCount) {
                val value = UUID.randomUUID().toString()

                index.add(value, value)

                map.add(value)
            }
        }
        println("Time taken to add $itemCount elements: $timeTaken")
        var found = 0
        val timeTakenSearch1 = measureTime {
            map.forEach {  value ->
                val searchMustInclude = index.searchMustInclude(listOf(listOf(value))){
                    it == value
                }
                if (value in searchMustInclude) {
                    found++
                }else{
                    throw IllegalStateException("Not finding the entry")
                }
            }
        }
        println("Time taken to search $itemCount elements: $timeTakenSearch1, average ${(timeTakenSearch1.inWholeNanoseconds / found).nanoseconds}")
        println("not hr: "+index.serializeToBytes().size.printBytesAsAppropriateUnit())
        index.convertToHigherRank()
        println("hr: "+index.serializeToBytes().size.printBytesAsAppropriateUnit())
        found = 0
       val timeTakenSearch = measureTime {
            map.forEach { value ->
                val searchMustInclude = index.searchMustInclude(listOf(listOf(value))) {
                    it == value
                }
                if (value in searchMustInclude) {
                    found++
                }else{
                    throw IllegalStateException("Not finding the entry")
                }
            }
        }
        println("Time taken to search higher rank $itemCount elements: $timeTakenSearch, average ${(timeTakenSearch.inWholeNanoseconds / found).nanoseconds}")

    }
}

