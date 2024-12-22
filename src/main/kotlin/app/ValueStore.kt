package app

import State
import deserializeJsonToObject
import util.Index
import java.io.Serializable
import java.time.OffsetDateTime


const val cap = 10_000

class ValueStore : Serializable {
    private val indexes = mutableListOf(Index<Domain>())
    private val indexesCache = mutableListOf<List<Domain>>()
    var size = 0

    fun put(seq: Long, v: String, indexIdentifier: String) {
        getLogJson(v, indexIdentifier, seq)?.let {
            size++
            if (indexes.last().size >= cap) {
                indexes.last().convertToHigherRank()
                indexes.add(Index())
                indexesCache.add(mutableListOf())
            }
            State.indexedLines.addAndGet(1)
            indexes.last().add(it)
        }
    }

    private fun getLogJson(v: String, indexIdentifier: String, seq: Long): Domain? {
        return try {
            val domain = v.substringAfter(" ").deserializeJsonToObject<Domain>()
            domain.indexIdentifier = indexIdentifier
            domain.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant()
            domain.seq = seq
            domain.init()
            domain
        } catch (e: Exception) {
            val domain = Domain(seq = seq, message = v.substringAfter(" "), application = indexIdentifier)
            domain.indexIdentifier = indexIdentifier
            try {
                domain.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant()
                domain.init()
            } catch (e: Exception) {
                return null
            }
            domain
        }
    }

    fun search(query: String, length: Int, offsetLock: Long): List<Domain> {
        val liveIndexResults = getLiveIndexResults(getQuery(query), offsetLock, length)
        return if (liveIndexResults.size >= length) {
            liveIndexResults.take(length)
        } else {
            (liveIndexResults + getRankedListResults(
                getQuery(query),
                offsetLock,
                length - liveIndexResults.size
            )).take(length)
        }
    }

    private fun getRankedListResults(
        q: Query,
        offsetLock: Long,
        length: Int
    ): List<Domain> {
        return indexes.dropLast(1).reversed().asSequence()
            .flatMap { index ->
                index.searchMustInclude(q.filteredQueryList) { domain ->
                    domain.seq <= offsetLock && contains(q.queryList, q.queryListNot, domain)
                }.take(length)
            }.take(length)
            .toList()
    }

    private fun getLiveIndexResults(
        q: Query,
        offsetLock: Long,
        length: Int
    ) = indexes.last().searchMustInclude(q.filteredQueryList) {
        it.seq <= offsetLock && contains(q.queryList, q.queryListNot, it)
    }.sortedByDescending { it.timestamp }.take(length)
        .toList()

    data class Query(
        val queryListNot: List<String>,
        var queryList: List<String>,
        var filteredQueryList: List<List<String>>
    )

    private fun getQuery(
        query: String,
    ): Query {
        val queryListNot = mutableListOf<String>()
        val queryList = mutableListOf<String>()

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
        return Query(
            queryListNot = queryListNot,
            queryList = queryList,
            filteredQueryList = listOf(queryList.filter { it.isNotBlank() })
        )
    }

    private fun contains(
        queryList: List<String>,
        queryListNot: List<String>,
        domain: Domain
    ): Boolean {
        val contains =
            if (queryList.isEmpty() && queryListNot.isEmpty()) {
                true
            } else if (queryList.isEmpty()) {
                queryListNot.none {
                    domain.toString().contains(
                        it,
                        true
                    )
                }
            } else if (queryListNot.isEmpty()) {
                queryList.all { domain.toString().contains(it, true) }
            } else {
                queryList.all { domain.toString().contains(it, true) } && queryListNot.none {
                    domain.toString().contains(
                        it,
                        true
                    )
                }
            }
        return contains
    }

}
