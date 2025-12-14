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

data class IndexBlock(
    val index: Index<DomainLine>,
    val drainTree: DrainTree,
    var minSeq: Long = 0,
    var maxSeq: Long = Long.MAX_VALUE,
    var maxTimestamp: Long = 0
)


class ValueStore {
    private val levelIndexes = mutableMapOf<LogLevel, MutableList<IndexBlock>>()
    var size = 0

    fun getLogClusters(): List<LogCluster> = State.levels.get().flatMap { level ->
        levelIndexes[level]?.flatMap { (_, drainTree) ->
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
        val level = domainLine.level
        val levelIndexList =
            levelIndexes.getOrPut(level) { mutableListOf(IndexBlock(Index(), DrainTree(domainLine.indexIdentifier))) }
        run {
            val indexBlock = levelIndexList.last()
            if (indexBlock.index.size >= cap) {
                indexBlock.index.convertToHigherRank()
                indexBlock.drainTree.final()
                levelIndexList.add(IndexBlock(Index(), DrainTree(domainLine.indexIdentifier)))
            }
        }
        val indexBlock = levelIndexList.last()
        if (domainLine.timestamp < indexBlock.maxTimestamp) {
            return
        }
        if (domainLine is LogLineDomain) {
            indexBlock.drainTree.add(domainLine)
        }
        if (indexBlock.index.size == 0) {
            indexBlock.minSeq = domainLine.seq
        }
        indexBlock.index.add(domainLine, domainLine.toString())
        indexBlock.maxSeq = seq.get()
        indexBlock.maxTimestamp = domainLine.timestamp
        size++
        State.indexedLines.addAndGet(1)
    }

    fun search(query: String, offsetLock: Long): Sequence<DomainLine> {
        val q = getQuery(query)
        val l = offsetLock - State.lock.get()
        // Search each specified level
        return State.levels.get().mapNotNull { level ->
            levelIndexes[level]?.reversed()?.asSequence()
                ?.filter { it.minSeq < l }
                ?.filter { it.minSeq < offsetLock }
                ?.map { (index, _, min, max) ->
                    index.searchMustInclude(q.filteredQueryList) {
                        (it.seq <= l && it.seq <= offsetLock && it.contains(q.queryList, q.queryListNot))
                    }
                }?.reduceOrNull { acc, seq -> acc + seq }
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
            queryListNot = queryListNot.filter { it.isNotBlank() },
            queryList = queryList.filter { it.isNotBlank() },
            filteredQueryList = listOf(queryList.filter { it.isNotBlank() })
        )
    }
}

