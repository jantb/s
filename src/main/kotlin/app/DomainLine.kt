package app

import LogLevel
import java.io.Serializable

sealed interface DomainLine :Serializable, Comparable<DomainLine> {
    val seq: Long
    val level: LogLevel
    val timestamp: Long
    val message: String
    val indexIdentifier: String
    fun contains(
    queryList: List<String>,
    queryListNot: List<String>,
    ): Boolean
}