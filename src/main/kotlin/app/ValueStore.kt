package app

import State
import deserializeJsonToObject
import util.CountMin
import util.Index
import java.io.Serializable
import java.time.OffsetDateTime


val cap = 100_000

class ValueStore : Serializable {
    private val indexes = mutableListOf(Index<LogJson>())
    var size = 0

    fun put(seq: Long, v: String, indexIdentifier: String) {
        getLogJson(v, indexIdentifier, seq)?.let {
            val value = it.searchableString()
            size++
            if (indexes.last().size >= cap) {
                indexes.last().convertToHigherRank()
                indexes.add(Index())
            }
            State.indexedLines.addAndGet(1)
            indexes.last().add(it, value)
        }
    }

    private fun getLogJson(v: String, indexIdentifier: String, seq: Long): LogJson? {
        return try {
            val logJson = v.substringAfter(" ").deserializeJsonToObject<LogJson>()
            logJson.indexIdentifier = indexIdentifier
            logJson.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant()
            logJson.seq = seq
            logJson.init()
            logJson
        } catch (e: Exception) {
            val logJson = LogJson(seq = seq, message = v.substringAfter(" "), application = indexIdentifier)
            logJson.indexIdentifier = indexIdentifier
            try {
                logJson.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant()
                logJson.init()
            } catch (e: Exception) {
                return  null
            }
            logJson
        }
    }

    fun search(query: String, length: Int, offsetLock: Long): List<Domain> {

        val queryList = mutableListOf<String>()
        val queryListNot = mutableListOf<String>()
        var isInPhrase = false
        var phrase = ""

        query.split(" ").forEach { word ->
            if (word.startsWith("!") && !isInPhrase) {
                queryListNot.add(word.substring(1))
            } else if (word.startsWith("\"") && !isInPhrase) {
                isInPhrase = true
                phrase = word.substring(1)
            } else if (word.endsWith("\"") && isInPhrase) {
                phrase += " $word"
                phrase = phrase.substring(0, phrase.length - 1)
                queryList.add(phrase)
                isInPhrase = false
            } else if (isInPhrase) {
                phrase += " $word"
            } else {
                queryList.add(word)
            }
        }


        return indexes.reversed().asSequence()
            .map { index -> index.searchMustInclude(listOf(queryList.filter { it.isNotBlank() })) }.flatten()
            .filter { domain ->
                domain.seq <= offsetLock
            }
            .filter { domain ->
                val contains =
                    if (queryList.isEmpty() && queryListNot.isEmpty()) {
                        true
                    } else if (queryList.isEmpty()) {
                        queryListNot.none {
                            domain.searchableString().contains(
                                it,
                                true
                            )
                        }
                    } else if (queryListNot.isEmpty()) {
                        queryList.all { domain.searchableString().contains(it, true) }
                    } else {
                        queryList.all { domain.searchableString().contains(it, true) } && queryListNot.none {
                            domain.searchableString().contains(
                                it,
                                true
                            )
                        }
                    }

                contains
            }.sortedByDescending { it.timestamp }.take(length)
            .toList().reversed()
    }

}
