package app

import LogLevel
import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable

@Serializable
sealed interface DomainLine : Comparable<DomainLine> {
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