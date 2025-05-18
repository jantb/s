import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import util.BitCask

class BitCaskTest {

    @Test
    fun `should store and retrieve a value by key`() {
        val tempDir = Files.createTempDirectory("bitcask_test")
        val bitCask = BitCask(tempDir)

        val key = "testKey"
        val value = "testValue".toByteArray()

        bitCask[key] = value
        val retrievedValue = bitCask[key]

        assertNotNull(retrievedValue)
        assertArrayEquals(value, retrievedValue)

        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }

    @Test
    fun `should return null when retrieving non-existent key`() {
        val tempDir = Files.createTempDirectory("bitcask_test")
        val bitCask = BitCask(tempDir)

        val nonExistentKey = "nonExistentKey"

        val retrievedValue = bitCask[nonExistentKey]

        assertNull(retrievedValue)

        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }

    @Test
    fun `should remove a key-value pair`() {
        val tempDir = Files.createTempDirectory("bitcask_test")
        val bitCask = BitCask(tempDir)

        val key = "testKey"
        val value = "testValue".toByteArray()

        bitCask[key] = value
        val removed = bitCask.remove(key)

        assertTrue(removed)
        assertNull(bitCask[key])

        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }

    @Test
    fun `should return false when removing non-existent key`() {
        val tempDir = Files.createTempDirectory("bitcask_test")
        val bitCask = BitCask(tempDir)

        val nonExistentKey = "nonExistentKey"

        val removed = bitCask.remove(nonExistentKey)

        assertFalse(removed)

        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }

    @Test
    fun `should clear all data`() {
        val tempDir = Files.createTempDirectory("bitcask_test")
        val bitCask = BitCask(tempDir)

        val key = "testKey"
        val value = "testValue".toByteArray()

        bitCask[key] = value
        bitCask.clear()

        assertNull(bitCask[key])

        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }

    @Test
    fun `should compact storage and maintain data`() {
        val tempDir = Files.createTempDirectory("bitcask_test")
        val bitCask = BitCask(tempDir)

        val key1 = "key1"
        val value1 = "value1".toByteArray()

        val key2 = "key2"
        val value2 = "value2".toByteArray()

        bitCask[key1] = value1
        bitCask[key2] = value2

        bitCask.compact()

        assertArrayEquals(value1, bitCask[key1])
        assertArrayEquals(value2, bitCask[key2])

        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
}