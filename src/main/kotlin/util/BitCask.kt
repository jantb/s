package util

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

class BitCask(private val dataDir: Path) {
    private val storagePath = "storage.dat"
    private val entriesPath = "entries.dat"
    private val deletionsPath = "deletions.dat"

    private val index = mutableMapOf<String, Long>()

    @Volatile
    private var writeCountSinceLastCompact = 0
    private val compactThreshold = 1000
    private val compactLock = Any()

    private var dataFile: RandomAccessFile
    private var entriesFileChannel: FileChannel
    private var deletionsFileChannel: FileChannel

    init {
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir)
        }

        dataFile = RandomAccessFile(dataDir.resolve(storagePath).toFile(), "rw")
        entriesFileChannel = FileChannel.open(
            dataDir.resolve(entriesPath),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        )
        deletionsFileChannel = FileChannel.open(
            dataDir.resolve(deletionsPath),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        )

        loadIndex()

        Runtime.getRuntime().addShutdownHook(Thread {
            closeFiles()
        })
    }

    private fun closeFiles() {
        dataFile.close()
        entriesFileChannel.close()
        deletionsFileChannel.close()
    }

    fun clear() {
        closeFiles()
        Files.deleteIfExists(dataDir.resolve(storagePath))
        Files.deleteIfExists(dataDir.resolve(entriesPath))
        Files.deleteIfExists(dataDir.resolve(deletionsPath))
        index.clear()
        dataFile = RandomAccessFile(dataDir.resolve(storagePath).toFile(), "rw")
        entriesFileChannel = FileChannel.open(dataDir.resolve(entriesPath), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
        deletionsFileChannel = FileChannel.open(dataDir.resolve(deletionsPath), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
    }

    private fun loadIndex() {
        val entriesFile = dataDir.resolve(entriesPath)
        val deletionsFile = dataDir.resolve(deletionsPath)

        if (Files.exists(entriesFile)) {
            FileChannel.open(entriesFile, StandardOpenOption.READ).use { channel ->
                val buffer = ByteBuffer.allocate(channel.size().toInt())
                channel.read(buffer)
                buffer.flip()

                while (buffer.hasRemaining()) {
                    val keySize = buffer.int
                    val keyBytes = ByteArray(keySize)
                    buffer.get(keyBytes)
                    val offset = buffer.long
                    val key = keyBytes.decodeToString()
                    index[key] = offset
                }
            }
        }

        if (Files.exists(deletionsFile)) {
            FileChannel.open(deletionsFile, StandardOpenOption.READ).use { channel ->
                val buffer = ByteBuffer.allocate(channel.size().toInt())
                channel.read(buffer)
                buffer.flip()

                while (buffer.hasRemaining()) {
                    val keySize = buffer.int
                    val keyBytes = ByteArray(keySize)
                    buffer.get(keyBytes)
                    val key = keyBytes.decodeToString()
                    index.remove(key)
                }
            }
        }
    }

    operator fun set(key: String, value: ByteArray) {
        val keyBytes = key.encodeToByteArray()
        val buffer = ByteBuffer.allocate(Int.SIZE_BYTES + value.size)
        buffer.putInt(value.size)
        buffer.put(value)
        buffer.flip()

        val offset = dataFile.length()
        dataFile.seek(offset)
        dataFile.channel.write(buffer)

        val entryBuffer = ByteBuffer.allocate(Int.SIZE_BYTES + keyBytes.size + Long.SIZE_BYTES)
        entryBuffer.putInt(keyBytes.size)
        entryBuffer.put(keyBytes)
        entryBuffer.putLong(offset)
        entryBuffer.flip()
        entriesFileChannel.write(entryBuffer)

        index[key] = offset
        maybeCompact()
    }

    operator fun get(key: String): ByteArray? {
        val offset = index[key] ?: return null
        dataFile.seek(offset)

        val valueSize = dataFile.readInt()
        val valueBytes = ByteArray(valueSize)
        dataFile.readFully(valueBytes)

        return valueBytes
    }

    fun remove(key: String): Boolean {
        val keyBytes = key.encodeToByteArray()

        val buffer = ByteBuffer.allocate(Int.SIZE_BYTES + keyBytes.size)
        buffer.putInt(keyBytes.size)
        buffer.put(keyBytes)
        buffer.flip()
        deletionsFileChannel.write(buffer)

        val removed = index.remove(key) != null
        if (removed) maybeCompact()
        return removed
    }

    private fun maybeCompact() {
        writeCountSinceLastCompact++
        if (writeCountSinceLastCompact >= compactThreshold) {
            synchronized(compactLock) {
                if (writeCountSinceLastCompact >= compactThreshold) {
                    writeCountSinceLastCompact = 0
                    Thread { compact() }.start()
                }
            }
        }
    }

    @Synchronized
    fun compact() {
        val tempStorage = dataDir.resolve("storage_compact.dat")
        val tempEntries = dataDir.resolve("entries_compact.dat")

        RandomAccessFile(tempStorage.toFile(), "rw").use { newDataFile ->
            FileChannel.open(tempEntries, StandardOpenOption.CREATE, StandardOpenOption.WRITE).use { newEntriesChannel ->
                val newIndex = mutableMapOf<String, Long>()

                for ((key, oldOffset) in index) {
                    dataFile.seek(oldOffset)
                    val valueSize = dataFile.readInt()
                    val valueBytes = ByteArray(valueSize)
                    dataFile.readFully(valueBytes)

                    val newOffset = newDataFile.length()
                    newDataFile.seek(newOffset)
                    val valueBuffer = ByteBuffer.allocate(Int.SIZE_BYTES + valueBytes.size)
                    valueBuffer.putInt(valueSize)
                    valueBuffer.put(valueBytes)
                    valueBuffer.flip()
                    newDataFile.channel.write(valueBuffer)

                    val keyBytes = key.encodeToByteArray()
                    val entryBuffer = ByteBuffer.allocate(Int.SIZE_BYTES + keyBytes.size + Long.SIZE_BYTES)
                    entryBuffer.putInt(keyBytes.size)
                    entryBuffer.put(keyBytes)
                    entryBuffer.putLong(newOffset)
                    entryBuffer.flip()
                    newEntriesChannel.write(entryBuffer)

                    newIndex[key] = newOffset
                }

                closeFiles()
                Files.move(tempStorage, dataDir.resolve(storagePath), StandardCopyOption.REPLACE_EXISTING)
                Files.move(tempEntries, dataDir.resolve(entriesPath), StandardCopyOption.REPLACE_EXISTING)
                Files.deleteIfExists(dataDir.resolve(deletionsPath))

                dataFile = RandomAccessFile(dataDir.resolve(storagePath).toFile(), "rw")
                entriesFileChannel = FileChannel.open(dataDir.resolve(entriesPath),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
                deletionsFileChannel = FileChannel.open(dataDir.resolve(deletionsPath),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)

                index.clear()
                index.putAll(newIndex)
            }
        }
    }
}
