import java.io.*
import java.text.DecimalFormat
import kotlin.math.*

fun Int.round(value: Int): Int {
    if (value == 0) {
        return 0
    }
    return value * (abs(this.toDouble() / value).roundToInt())
}

fun Serializable.serializeToBytes(): ByteArray {
    ByteArrayOutputStream().use { byteArrayOutputStream ->
        ObjectOutputStream(byteArrayOutputStream).use { objectOutputStream ->
            objectOutputStream.writeObject(this)
        }
        return byteArrayOutputStream.toByteArray()
    }
}
fun Int.printBytesAsAppropriateUnit(): String {
    val units = arrayOf("B", "KB", "MB", "GB", "TB")
    if (this < 1024) {
        println("${this}B")
    } else {
        val digitGroups = (log10(this.toDouble()) / log10(1024.0)).toInt()
       return "${String.format("%.2f", this / 1024.0.pow(digitGroups.toDouble()))} ${units[digitGroups]}"
    }
    return ""
}

fun Int.format() = DecimalFormat("#,###").format(this)
    .replace(",", ".")
