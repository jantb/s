package util

import org.junit.jupiter.api.Assertions.*
import kotlin.test.Test

class RowTest {

    @Test
    fun `setBit should set correct bit in words array`() {
        val row = Row()
        row.setBit(7)
        assertEquals(1L shl 7, row.words[0])
    }

    @Test
    fun `isEmpty should return true for newly created Row`() {
        val row = Row()
        assertTrue(row.isEmpty())
    }

    @Test
    fun `isEmpty should return false if bit is set`() {
        val row = Row()
        row.setBit(7)
        assertFalse(row.isEmpty())
    }

    @Test
    fun `should correctly calculate all set bit positions`() {
        val row = Row()
        row.setBit(10)
        row.setBit(12)
        row.setBit(14)
        val setBitPositions = row.getAllSetBitPositions()
        assertEquals(listOf(10, 12, 14), setBitPositions)
    }

    @Test
    fun `should correctly calculate next set bit`() {
        val row = Row()
        row.setBit(42)
        assertEquals(42, row.nextSetBit(40))
    }

    @Test
    fun `should throw IllegalArgumentException for invalid targetDensity in conversionToTargetDensity`() {
        val row = Row()
        assertThrows(IllegalArgumentException::class.java) {
            row.convertToTargetDensity(1.5)
        }
    }

    @Test
    fun `double words test`() {
        // create instance of the class containing `doubleWords` method and initialize `words` and `wordsInUse`
        val row = Row()
        row.words = LongArray(3) { it.toLong() } // arbitrary initialization
        row.wordsInUse = row.words.size

        // call the function to test
        row.doubleWords()

        // assert the size of `words` is doubled
        assertEquals(3 * 2, row.words.size)

        // assert the words are correctly copied
        assertArrayEquals(LongArray(3) { it.toLong() }, row.words.sliceArray(0..2))
        assertArrayEquals(LongArray(3) { it.toLong() }, row.words.sliceArray(3..5))

        // assert the `wordsInUse` is updated
        assertEquals(3 * 2, row.wordsInUse)
    }
    @Test
    fun testDoubleWords() {
        val words = longArrayOf(1, 2, 3, 4, 5)
        val row = Row(words)
        row.doubleWords()
        assertArrayEquals(
            longArrayOf(1, 2, 3, 4, 5, 1, 2, 3, 4, 5),
            row.words
        )
    }
    @Test
    fun `double words when words is empty`() {
        // Create an instance of Row and initialize words as an empty array
        val row = Row()
        row.words = LongArray(0)
        row.wordsInUse = row.words.size

        // Call the function to test
        row.doubleWords()

        // Assert that the size of `words` is 0 as the initial array was empty
        assertEquals(0, row.words.size)

        // Assert that `wordsInUse` is 0 as there were no words in use initially
        assertEquals(0, row.wordsInUse)
    }

    @Test
    fun `double words when words has one element`() {
        // Create an instance of Row and initialize words with one element
        val row = Row()
        row.words = LongArray(1) { it.toLong() }
        row.wordsInUse = row.words.size

        // Call the function to test
        row.doubleWords()

        // Assert that the size of `words` is 2
        assertEquals(2, row.words.size)

        // Assert the words are correctly copied
        assertArrayEquals(LongArray(1) { it.toLong() }, row.words.sliceArray(0..0))
        assertArrayEquals(LongArray(1) { it.toLong() }, row.words.sliceArray(1..1))

        // Assert the `wordsInUse` is updated to 2
        assertEquals(2, row.wordsInUse)
    }


    @Test
    fun `double words large initial array`() {
        // Create an instance of Row and initialize words with a large number of elements
        val row = Row()
        row.words = LongArray(1000) { it.toLong() } // Arbitrary large array
        row.wordsInUse = row.words.size

        // Call the function to test
        row.doubleWords()

        // Assert the size of `words` is 2000
        assertEquals(2000, row.words.size)

        // Let's only test some of the words due to large size of the array
        assertArrayEquals(LongArray(10) { it.toLong() }, row.words.sliceArray(0..9))
        assertArrayEquals(LongArray(10) { it.toLong() }, row.words.sliceArray(1000..1009))

        // Assert the `wordsInUse` is updated
        assertEquals(2000, row.wordsInUse)
    }

    @Test
    fun `double words specific state`() {
        // Create an instance of Row and initialize words with a specific state, e.g., all elements are the same
        val row = Row()
        row.words = LongArray(10) { 5.toLong() } // Array filled with 5
        row.wordsInUse = row.words.size

        // Call the function to test
        row.doubleWords()

        // Assert the size of `words` is doubled
        assertEquals(20, row.words.size)

        // Assert the words are correctly copied
        assertArrayEquals(LongArray(10) { 5.toLong() }, row.words.sliceArray(0..9))
        assertArrayEquals(LongArray(10) { 5.toLong() }, row.words.sliceArray(10..19))

        // Assert the `wordsInUse` is updated
        assertEquals(20, row.wordsInUse)
    }
}