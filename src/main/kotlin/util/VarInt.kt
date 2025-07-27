package util

import kotlin.experimental.or

class VarInt {
    private var firstValue = 0L
    private var lastValue = 0L
    private var count = 0

    // raw byte buffer + write pointer
    private var buf = ByteArray(128)
    private var pos = 0

    // cache of the very last get()
    private var lastGetIndex = -1
    private var lastGetValue = 0L
    private var lastGetOffset = 0

    fun add(value: Long): Int {
        if (count == 0) {
            firstValue = value
            lastValue = value
            writeVar((value shl 1) xor (value shr 63))
        } else {
            val delta = value - lastValue
            lastValue = value
            writeVar((delta shl 1) xor (delta shr 63))
        }

        return count++
    }

    fun get(index: Int): Long {
        if (index < 0 || index >= count) {
            throw IllegalArgumentException("Index out of bounds: $index, size: $count")
        }

        // 1) exact cache hit?
        if (index == lastGetIndex) return lastGetValue

        // 2) sequential next?
        if (index == lastGetIndex + 1) {
            // decode exactly one var-int at lastGetO   ffset
            var raw = 0L
            var shift = 0
            var ptr = lastGetOffset
            while (true) {
                val b = buf[ptr++]
                raw = raw or ((b.toLong() and 0x7F) shl shift)
                if (b.toInt() and 0x80 == 0) break
                shift += 7
            }
            val delta = (raw ushr 1) xor -(raw and 1)
            val value = if (index == 0) delta else lastGetValue + delta

            // update cache
            lastGetIndex = index
            lastGetValue = value
            lastGetOffset = ptr

            return value
        }

        // 3) random access → decode from scratch
        var off = 0
        var result = 0L
        for (i in 0..index) {
            // inline readVar
            var raw = 0L
            var shift = 0
            var ptr = off
            while (true) {
                val b = buf[ptr++]
                raw = raw or ((b.toLong() and 0x7F) shl shift)
                if (b.toInt() and 0x80 == 0) break
                shift += 7
            }
            off = ptr
            val delta = (raw ushr 1) xor -(raw and 1)
            result = if (i == 0) delta else result + delta
        }

        // prime cache for next time
        lastGetIndex = index
        lastGetValue = result
        lastGetOffset = off

        return result
    }

    fun getCompressedData() =
        CompressedData(buf.copyOf(pos))

    private fun writeVar(value: Long) {
        var v = value
        // ensure space for up to 10 bytes
        if (pos + 10 >= buf.size) {
            buf = buf.copyOf(buf.size * 2)
        }
        while (true) {
            var b = (v and 0x7F).toByte()
            v = v ushr 7
            if (v != 0L) b = b or 0x80.toByte()
            buf[pos++] = b
            if (v == 0L) break
        }
    }

    data class CompressedData(
        val t64Bytes: ByteArray
    )
}
