package util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.random.Random
import kotlin.system.measureTimeMillis

class T64CompressionTest {

    @Test
    fun `test empty compressor`() {
        val compressor = T64Compression()
        val compressed = compressor.getCompressedData()
        assertEquals(0, compressed.t64Bytes.size)
        assertThrows<IllegalArgumentException> { compressor.get(0) }
    }

    @Test
    fun `test single value`() {
        val compressor = T64Compression()
        compressor.add(100L)
        val compressed = compressor.getCompressedData()
        assertTrue(compressed.t64Bytes.isNotEmpty())
        assertEquals(100L, compressor.get(0))
    }

    @Test
    fun `test positive and negative numbers`() {
        val compressor = T64Compression()
        val numbers = listOf<Long>(
            100, 105, 108, 109, 108, 105, 100, -100, -105, -108
        )
        numbers.forEach { compressor.add(it) }

        val compressed = compressor.getCompressedData()


        // Verify T64 bytes are non-empty and reasonable size
        assertTrue(compressed.t64Bytes.isNotEmpty())

        // Verify all values can be retrieved
        numbers.forEachIndexed { index, expected ->
            val actual = compressor.get(index)
            assertEquals(expected, actual, "Value at index $index")
        }
    }

    @Test
    fun `test large numbers`() {
        val compressor = T64Compression()
        val numbers = listOf<Long>(
            Long.MAX_VALUE, Long.MAX_VALUE - 5, Long.MAX_VALUE - 10,
            Long.MIN_VALUE, Long.MIN_VALUE + 5, Long.MIN_VALUE + 10
        )
        numbers.forEach { compressor.add(it) }

        val compressed = compressor.getCompressedData()
        assertTrue(compressed.t64Bytes.isNotEmpty())

        // Verify retrieval
        numbers.forEachIndexed { index, expected ->
            val actual = compressor.get(index)
            assertEquals(expected, actual, "Value at index $index")
        }
    }

    @Test
    fun `test index out of bounds`() {
        val compressor = T64Compression()
        compressor.add(100L)
        compressor.add(200L)

        assertThrows<IllegalArgumentException> { compressor.get(-1) }
        assertThrows<IllegalArgumentException> { compressor.get(2) }
    }
    @Test
    fun `index test`() {
        val compressor = T64Compression()
        val first = compressor.add(100L)
        val second = compressor.add(200L)

        assertEquals(0, first)
        assertEquals(1, second)
        assertEquals(100L, compressor.get(first))
        assertEquals(200L, compressor.get(second))
    }

    @Test
    fun `test sequential additions`() {
        val compressor = T64Compression()
        val numbers = listOf<Long>(0, 10, 20, 30, 40, -10, -20, -30)
        numbers.forEach { compressor.add(it) }

        val compressed = compressor.getCompressedData()
        numbers.forEachIndexed { index, expected ->
            val actual = compressor.get(index)
            assertEquals(expected, actual, "Value at index $index")
        }
    }
    @Test
    fun `sequential performance 10M`() {
        val compressor = T64Compression()
        val N = 100_000_000
        val values = LongArray(N) { it * 10L }

        val addTime = measureTimeMillis {
            for (v in values) compressor.add(v)
        }
        println("Add time for $N values: $addTime ms")
        // print sizes
        val uncompressedSize = N.toLong() * Long.SIZE_BYTES
        val compressedSize = compressor.getCompressedData().t64Bytes.size
        println("Uncompressed size: $uncompressedSize bytes")
        println("Compressed size:   $compressedSize bytes")
        val getTime = measureTimeMillis {
            for (i in values.indices) {
                assertEquals(values[i], compressor.get(i))
            }
        }
        println("Get time for $N values: $getTime ms")
    }

    @Test
    fun `random performance and correctness with sizes`() {
        val compressor = T64Compression()
        val N = 100_000_000
        val rnd = Random(1234)
        val values = LongArray(N) { rnd.nextInt(0, 101).toLong() }

        val addTime = measureTimeMillis {
            for (v in values) compressor.add(v)
        }
        println("Random add time for $N values: $addTime ms")

        // print sizes
        val uncompressedSize = N.toLong() * Long.SIZE_BYTES
        val compressedSize = compressor.getCompressedData().t64Bytes.size
        println("Uncompressed size: $uncompressedSize bytes")
        println("Compressed size:   $compressedSize bytes")

        val getTime = measureTimeMillis {
            for (i in values.indices) {
                val get = compressor.get(i)
                assertEquals(values[i], get)
            }
        }
        println("Random get time for $N values: $getTime ms")
    }
}