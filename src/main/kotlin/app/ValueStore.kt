package app

import DrainTree
import LogCluster
import LogLevel
import State
import parallelSortWith
import util.Index
import java.util.concurrent.atomic.AtomicLong


const val cap = 512
val seq  =  AtomicLong(0)
class ValueStore {
    private val levelIndexes = mutableMapOf<LogLevel, MutableList<Pair<Index<Int>, DrainTree>>>()
    var size = 0

    fun getLogClusters(): List<LogCluster> = State.levels.get().flatMap { level ->
        levelIndexes[level]?.flatMap { (index, drainTree) ->
            drainTree.logClusters().map { cluster ->
                cluster.copy(indexIdentifier = drainTree.indexIdentifier)
            }
        } ?: emptyList()
    }.groupBy { it.level to it.block }
        .map { (key, group) ->
            val (level, block) = key
            val totalCount = group.sumOf { it.count }
            LogCluster(totalCount, level, block, group.first().indexIdentifier)
        }

    fun put(domainLine: DomainLine) {
        indexDomainLine(domainLine)
    }

    private fun indexDomainLine(it: DomainLine) {
        size++
        State.indexedLines.addAndGet(1)

        val level = it.level

        val levelIndexList =
            levelIndexes.getOrPut(level) { mutableListOf(Index<Int>() to DrainTree(it.indexIdentifier)) }
        run {
            val (index, drain) = levelIndexList.last()
            if (index.size >= cap) {
                index.convertToHigherRank()
                drain.final()
                levelIndexList.add(Index<Int>() to DrainTree(it.indexIdentifier))
            }
        }
        val (index, drain) = levelIndexList.last()
        val drainIndex = drain.add(it)
        index.add(drainIndex, it.toString())
    }

    suspend fun search(query: String, length: Int, offsetLock: Long): List<DomainLine> {
        val q = getQuery(query)

        // If levels are specified, search only those level indexes
        val results = mutableListOf<DomainLine>()

        // Search each specified level
        val logLevels = State.levels.get().toSet()
        for (level in logLevels) {
            val levelIndexList = levelIndexes[level] ?: continue

            // Search the live index for this level
            if (levelIndexList.isNotEmpty()) {
                val (index, drain) = levelIndexList.last()
                val liveResults = index.searchMustInclude(q.filteredQueryList) {
                    val domain = drain.get(it)
                    (domain.seq <= offsetLock && domain.contains(q.queryList, q.queryListNot)) to domain
                }.take(length).toList()

                results.addAll(liveResults)

                // If we need more results, search the ranked indexes for this level
                if (liveResults.size < length) {
                    val rankedResults = levelIndexList.dropLast(1).reversed().asSequence()
                        .flatMap { (index, drain) ->
                            index.searchMustInclude(q.filteredQueryList) {
                                val domain = drain.get(it)
                                (domain.seq <= offsetLock && domain.contains(q.queryList, q.queryListNot)) to domain
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
}

