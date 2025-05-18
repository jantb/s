package util

import java.io.*

fun Serializable.toBytes(): ByteArray {
    ByteArrayOutputStream().use { byteStream ->
        ObjectOutputStream(byteStream).use { objStream ->
            objStream.writeObject(this)
        }
        return byteStream.toByteArray()
    }
}

inline fun <reified T : Serializable> ByteArray.fromBytes(): T {
    ByteArrayInputStream(this).use { byteStream ->
        ObjectInputStream(byteStream).use { objStream ->
            return objStream.readObject() as T
        }
    }
}