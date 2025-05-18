package util

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

class BitCaskByOffset(
    private val dataDir: Path,
) {
    private val storagePath = "storage.dat"
    private val dataFile: RandomAccessFile
    private val cache: LinkedHashMap<Long, ByteArray>

    init {
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir)
        }

        dataFile = RandomAccessFile(dataDir.resolve(storagePath).toFile(), "rw")

        // Initialize LRU cache with capacity of 100,000
        cache = object : LinkedHashMap<Long, ByteArray>(100_000, 0.75f, true) {
            override fun removeEldestEntry(eldest: Map.Entry<Long, ByteArray>): Boolean {
                return size > 100_000
            }
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            closeFiles()
        })
    }

    private fun closeFiles() {
        dataFile.close()
    }

    fun set(value: ByteArray): Long {
        val offset = dataFile.length()
        dataFile.seek(offset)

        val buffer = ByteBuffer.allocate(Int.SIZE_BYTES + value.size)
        buffer.putInt(value.size)
        buffer.put(value)
        buffer.flip()
        dataFile.channel.write(buffer)

        // Add to cache
        cache[offset] = value

        return offset
    }

    operator fun get(offset: Long): ByteArray? {
        // Check cache first
        cache[offset]?.let { return it }

        // Read from file if not in cache
        return try {
            dataFile.seek(offset)
            val valueSize = dataFile.readInt()
            val valueBytes = ByteArray(valueSize)
            dataFile.readFully(valueBytes)

            // Add to cache
            cache[offset] = valueBytes.copyOf()
            valueBytes
        } catch (e: Exception) {
            null
        }
    }
}