package util

import java.util.LinkedHashMap

class T64Compression(capacity: Int = 128) {
    private var lastValue: Long = 0L
    private var count = 0
    private var buf = ByteArray(capacity)
    private var pos = 0
    private var block = LongArray(64)
    private var blockSize = 0
    private var currentSum = 0L
    private var prefixSumForNextBlock = 0L
    private var blockStarts = mutableListOf<Int>()

    // Cache for previously decoded blocks (LRU)
    private class LruCache<K, V>(private val maxEntries: Int) {
        private val map = object : LinkedHashMap<K, V>(maxEntries, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean = size > maxEntries
        }

        fun get(key: K): V? = map[key]
        fun put(key: K, value: V) {
            map[key] = value
        }
    }

    private val decodedBlocksCache = LruCache<Int, BlockCacheEntry>(maxEntries = 256)

    fun add(value: Long): Int {
        val delta = if (count == 0) value else value - lastValue
        lastValue = value
        val unsignedDelta = (delta shl 1) xor (delta shr 63)
        block[blockSize] = unsignedDelta
        blockSize++
        currentSum += delta
        if (blockSize == 64) {
            encodeBlock()
        }
        return count++
    }

    private fun encodeBlock() {
        if (blockSize == 0) return
        // Record start position of the block
        blockStarts.add(pos)
        // Ensure space for prefix sum (8 bytes) + block_size (1 byte) + bit_length (1 byte) + planes (bit_length * 8 bytes)
        val needed = 8 + 1 + 1 + (bitLength() * 8)
        if (pos + needed > buf.size) {
            buf = buf.copyOf(maxOf(buf.size * 2, pos + needed))
        }
        // Write prefix sum for this block (prefix of previous block)
        for (k in 0 until 8) {
            buf[pos] = ((prefixSumForNextBlock shr (k * 8)) and 0xFF).toByte()
            pos++
        }
        // Write block_size
        buf[pos++] = blockSize.toByte()
        // Compute and write bit_length
        val bit_length = bitLength()
        buf[pos++] = bit_length.toByte()
        // Write planes
        for (j in 0 until bit_length) {
            var plane = 0L
            for (i in 0 until blockSize) {
                if ((block[i] and (1L shl j)) != 0L) {
                    plane = plane or (1L shl i)
                }
            }
            // Write plane as 8 bytes little-endian
            for (k in 0 until 8) {
                buf[pos] = ((plane shr (k * 8)) and 0xFF).toByte()
                pos++
            }
        }
        // Update prefixSumForNextBlock to currentSum after encoding this block
        prefixSumForNextBlock = currentSum
        // Reset
        blockSize = 0

        // Keep cache: existing decoded blocks are still valid; for the newly encoded block
        // we will "replace on update" when/if it gets decoded via get().
    }

    private fun bitLength(): Int {
        var msbMax = -1
        for (i in 0 until blockSize) {
            if (block[i] != 0L) {
                val lz = block[i].countLeadingZeroBits()
                val msb_i = 63 - lz
                if (msb_i > msbMax) msbMax = msb_i
            }
        }
        return if (msbMax == -1) 0 else msbMax + 1
    }

    fun get(index: Int): Long {
        require(index in 0 until count)

        // Handle values in current unencoded block
        if (index >= count - blockSize) {
            val startIndex = count - blockSize
            var sum = prefixSumForNextBlock
            for (i in 0 until (index - startIndex + 1)) {
                val delta = (block[i] ushr 1) xor -(block[i] and 1L)
                sum += delta
            }
            return sum
        }

        val C = index / 64
        val k = index % 64

        decodedBlocksCache.get(C)?.let { entry ->
            return entry.prefixSum + entry.cumulative[k]
        }

        val startPos = blockStarts[C]
        var bufPos = startPos
        var prefixSum = 0L
        for (m in 0 until 8) {
            prefixSum = prefixSum or ((buf[bufPos].toLong() and 0xFF) shl (m * 8))
            bufPos++
        }
        val readBlockSize = buf[bufPos++].toInt() and 0xFF
        val bitLength = buf[bufPos++].toInt() and 0xFF

        val planes = LongArray(bitLength)
        for (j in 0 until bitLength) {
            var plane = 0L
            for (m in 0 until 8) {
                plane = plane or ((buf[bufPos++].toLong() and 0xFF) shl (m * 8))
            }
            planes[j] = plane
        }

        val cumulative = LongArray(readBlockSize)
        var sum = 0L
        for (p in 0 until readBlockSize) {
            var unsignedDelta = 0L
            for (j in 0 until bitLength) {
                if ((planes[j] and (1L shl p)) != 0L) {
                    unsignedDelta = unsignedDelta or (1L shl j)
                }
            }
            val delta = (unsignedDelta ushr 1) xor -(unsignedDelta and 1L)
            sum += delta
            cumulative[p] = sum
        }

        // Replace-on-update: if the block already exists in cache, overwrite it.
        decodedBlocksCache.put(C, BlockCacheEntry(prefixSum, cumulative))

        return prefixSum + cumulative[k]
    }

    fun getCompressedData(): CompressedData {
        if (blockSize > 0) encodeBlock()
        return CompressedData(buf.copyOf(pos))
    }

    data class CompressedData(val t64Bytes: ByteArray)
    data class BlockCacheEntry(val prefixSum: Long, val cumulative: LongArray)
}