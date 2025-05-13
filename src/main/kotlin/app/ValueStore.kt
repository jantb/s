package app

import DrainTree
import LogCluster
import LogLevel
import State
import merge
import util.Index
import java.util.concurrent.atomic.AtomicLong


const val cap = 512
val seq = AtomicLong(0)

class ValueStore {
    private val levelIndexes = mutableMapOf<LogLevel, MutableList<Pair<Index<DomainLine>, DrainTree>>>()
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
            levelIndexes.getOrPut(level) { mutableListOf(Index<DomainLine>() to DrainTree(it.indexIdentifier)) }
        run {
            val (index, drain) = levelIndexList.last()
            if (index.size >= cap) {
                index.convertToHigherRank()
                drain.final()
                levelIndexList.add(Index<DomainLine>() to DrainTree(it.indexIdentifier))
            }
        }
        val (index, drain) = levelIndexList.last()
         drain.add(it)
        index.add(it, it.toString())
    }

    fun search(query: String, offsetLock: Long): Sequence<DomainLine> {
        val q = getQuery(query)

        // Search each specified level
        return State.levels.get().toSet().mapNotNull { level ->
            levelIndexes[level]?.reversed()
                ?.map { (index, drain) ->
                    index.searchMustInclude(q.filteredQueryList) {
                        val contains = it.contains(q.queryList, q.queryListNot)
                        (it.seq <= offsetLock && contains) to it
                    }
                }?.merge()
        }.merge()
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

