package index

import java.io.Serializable
import java.lang.Long
import kotlin.Double
import kotlin.IllegalArgumentException
import kotlin.Int
import kotlin.LongArray
import kotlin.math.abs

class Row(initialSize: Int = 1) : Serializable {
    var bitArray = LongArray(initialSize)
    var rank = 0

    fun convertToTargetDensity(targetDensity: Double) {
        if (targetDensity < 0.0 || targetDensity > 1.0) {
            throw IllegalArgumentException("Target density should be between 0.0 and 1.0.")
        }

        while (bitArray.size > 8) {
            val length = bitArray.size
            val partSize = length / 2

            val newArray = LongArray(partSize) {
                bitArray[it] or bitArray[it + partSize]
            }

            val newDensity = newArray.calculateDensity()
            if (abs(newDensity - targetDensity) >= abs(bitArray.calculateDensity() - targetDensity)) {
                break
            }

            bitArray = newArray
            rank++
        }
    }

    fun convertToRank(targetRank: Int) {
        while (bitArray.size > 1 && rank < targetRank) {
            val length = bitArray.size
            val partSize = length / 2

            val newArray = LongArray(partSize) {
                bitArray[it] or bitArray[it + partSize]
            }

            bitArray = newArray
            rank++
        }
    }

    fun setBit(bitToSet: Int) {
        if (bitToSet < 0) {
            throw IllegalArgumentException("Bit index should be non-negative.")
        }

        expandToFitBit(bitToSet)

        val originalIndex = bitToSet / Long.SIZE
        bitArray[originalIndex] = bitArray[originalIndex] or (1L shl (bitToSet % Long.SIZE))
    }

    fun expandToFitBit(bitToSet: Int) {
        if (bitToSet < 0) {
            throw IllegalArgumentException("Bit index should be non-negative.")
        }
        if (bitArray.isEmpty()) {
            bitArray = LongArray(1)
        }
        var partSize = bitArray.size / (1 shl 0)
        while (bitToSet >= partSize * Long.SIZE) {
            growInputArray()
            partSize *= 2
        }
    }

    private fun growInputArray() {
        val newArray = LongArray(bitArray.size * 2)
        System.arraycopy(bitArray, 0, newArray, 0, bitArray.size)
        bitArray = newArray
    }
}
fun LongArray.calculateDensity() = sumOf { java.lang.Long.bitCount(it) }.toDouble() / (size * java.lang.Long.SIZE)
