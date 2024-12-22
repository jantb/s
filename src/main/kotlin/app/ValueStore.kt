package app

import State
import deserializeJsonToObject
import util.Index
import java.io.Serializable
import java.time.OffsetDateTime


const val cap = 10_000

class ValueStore : Serializable {
    private val indexes = mutableListOf(Index<LogJson>())
    private val indexesCache = mutableListOf<List<LogJson>>()
    var size = 0

    fun put(seq: Long, v: String, indexIdentifier: String) {
        getLogJson(v, indexIdentifier, seq)?.let {
            val value = it.searchableString()
            size++
            if (indexes.last().size >= cap) {
                indexes.last().convertToHigherRank()
                indexes.add(Index())
                indexesCache.add(mutableListOf())
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
                return null
            }
            logJson
        }
    }

    private var prevSearch = ""
    private var prevSearchCached = emptyList<Domain>()
    private var indexesLength = 1
    private var prevQ= Query(
        queryListNot = emptyList(),
        queryList =  emptyList(),
        filteredQueryList =  emptyList()
    )


    fun search(query: String, length: Int, offsetLock: Long): List<Domain> {
        return if (query == prevSearch && indexesLength == indexes.size){
            prevQ
            val list = getLiveIndexResults(prevQ, offsetLock, length)
            (list + prevSearchCached).reversed()
        }else{
            prevQ =getQuery(query)
            val list = getLiveIndexResults(prevQ, offsetLock, length)
            val rankedList = getRankedListResults(prevQ, offsetLock, length)
            prevSearchCached = rankedList
            indexesLength = indexes.size
            (list + rankedList).reversed()
        }
    }

    private fun getRankedListResults(
        q: Query,
        offsetLock: Long,
        length: Int
    ) = indexes.dropLast(1).reversed().asSequence()
        .flatMap { index ->
            index.searchMustInclude(q.filteredQueryList)
        }
        .filter { domain ->
            domain.seq <= offsetLock && contains(q.queryList, q.queryListNot, domain)
        }

        .sortedByDescending { it.timestamp }.take(length)
        .toList()

    private fun getLiveIndexResults(
        q: Query,
        offsetLock: Long,
        length: Int
    ) = indexes.last().searchMustInclude(q.filteredQueryList).filter {
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


        val queryListNot =  mutableListOf<String>()
        val queryList =  mutableListOf<String>()

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
        return Query(queryListNot = queryListNot, queryList = queryList, filteredQueryList =  listOf(queryList.filter { it.isNotBlank() }))
    }

    private fun contains(
        queryList: List<String>,
        queryListNot: List<String>,
        domain: LogJson
    ): Boolean {
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
        return contains
    }

}
