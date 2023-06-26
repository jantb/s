package app

import State
import com.github.luben.zstd.Zstd
import deserializeJsonToObject
import index.Index
import java.io.Serializable
import java.time.OffsetDateTime
import java.util.*


val cap = 10_000

class ValueStore : Serializable {
    private val indexes = mutableListOf(Index())
    var size = 0

    @Transient
    private var prevQuery = ""
    fun put(key: Int, v: String) {
        val domain = try {
            val logJson = v.substringAfter(" ").deserializeJsonToObject<LogJson>()
            logJson.timestamp = OffsetDateTime.parse(v.substringBefore(" "))
            logJson
        } catch (e: Exception) {
            val logJson = LogJson(message = v.substringAfter(" "))
            logJson.timestamp = OffsetDateTime.parse(v.substringBefore(" "))
            logJson
        }
        val value = domain.searchableString()

        size++
        if (indexes.last().size >= cap) {
            indexes.last().convertToHigherRank()
            indexes.add(Index())
        }
        State.indexedLines.addAndGet(1)
        indexes.last().add(domain, value)
    }

    fun search(query: String, length: Int): List<Domain> {


        val queryList = query.split(" ").filter { !it.startsWith("!") }
        val queryListNot = query.split(" ").filter { it.startsWith("!") }.map { it.substring(1) }


        val res = mutableListOf<Domain>()
        val time = System.currentTimeMillis()
        indexes.reversed().asSequence().takeWhile { System.currentTimeMillis() - time <= 10 }
            .map { index -> index.searchMustInclude(queryList) }.flatten()
            .filter { pair ->
                val contains =
                    queryList.all { pair.searchableString().contains(it, true) } && queryListNot.none {
                        pair.searchableString().contains(
                            it,
                            true
                        )
                    }
                contains
            }
            .take(length)
            .forEach { res.add(it) }
        res.reverse()


        return res
    }

}
