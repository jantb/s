package app

import LogLevel
import deserializeJsonToObject
import parallelSortWith
import util.Index
import java.io.Serializable
import java.time.OffsetDateTime


const val cap = 512

class ValueStore : Serializable {
    private val levelIndexes = mutableMapOf<LogLevel, MutableList<Index<Domain>>>()
    var size = 0


    fun put(seq: Long, v: String, indexIdentifier: String) {
        getLogJson(v, indexIdentifier, seq)?.let {
            size++
            State.indexedLines.addAndGet(1)
            // Add to level-specific index
            val level = it.level


            val levelIndexList = levelIndexes.getOrPut(level) { mutableListOf(Index()) }
            if (levelIndexList.last().size >= cap) {
                levelIndexList.last().convertToHigherRank()
                levelIndexList.add(Index())
            }
            levelIndexList.last().add(it)
        }
    }

    private fun getLogJson(v: String, indexIdentifier: String, seq: Long): Domain? {
        return try {
            val domain = v.substringAfter(" ").deserializeJsonToObject<Domain>()
            domain.indexIdentifier = indexIdentifier
            domain.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant().toEpochMilli()
            domain.seq = seq
            domain.init()
            domain
        } catch (e: Exception) {
            val domain = Domain(seq = seq, message = v.substringAfter(" "), application = indexIdentifier)
            domain.indexIdentifier = indexIdentifier
            try {
                domain.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant().toEpochMilli()
                domain.init()
            } catch (e: Exception) {
                return null
            }
            domain
        }
    }

    suspend fun search(query: String, length: Int, offsetLock: Long, levels: Set<LogLevel>): List<Domain> {
        val q = getQuery(query)

        // If levels are specified, search only those level indexes
        val results = mutableListOf<Domain>()

        // Search each specified level
        for (level in levels) {
            val levelIndexList = levelIndexes[level] ?: continue

            // Search the live index for this level
            if (levelIndexList.isNotEmpty()) {
                val liveResults = levelIndexList.last().searchMustInclude(q.filteredQueryList) {
                    it.seq <= offsetLock && contains(q.queryList, q.queryListNot, it)
                }.take(length).toList()

                results.addAll(liveResults)

                // If we need more results, search the ranked indexes for this level
                if (liveResults.size < length) {
                    val rankedResults = levelIndexList.dropLast(1).reversed().asSequence()
                        .flatMap { index ->
                            index.searchMustInclude(q.filteredQueryList) {
                                it.seq <= offsetLock && contains(q.queryList, q.queryListNot, it)
                            }.take(length - liveResults.size)
                        }.take(length - liveResults.size)
                        .toList()

                    results.addAll(rankedResults)
                }
            }
        }

        // Sort and limit the results
        return results.parallelSortWith(compareByDescending { it.timestamp }).take(length)

    }

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
