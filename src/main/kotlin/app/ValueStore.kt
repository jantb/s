package app

import LogLevel
import State
import merge
import util.DrainCompressedDomainLineStore
import util.Index
import java.util.concurrent.atomic.AtomicLong

private const val INDEX_BLOCK_CAPACITY = 512 * 8
val globalSeq = AtomicLong(0)

data class IndexBlock(
    val index: Index<Long>,
    var minSeq: Long = 0,
    var maxSeq: Long = Long.MAX_VALUE,
    var maxTimestamp: Long = 0,
)

class ValueStore(
    private val drainStore: DrainCompressedDomainLineStore,
) {
    private val levelIndexes: MutableMap<LogLevel, MutableList<IndexBlock>> = mutableMapOf()

    var size: Int = 0
        private set

    suspend fun put(domainLine: DomainLine) {
        val blocksForLevel = levelIndexes.getOrPut(domainLine.level) {
            mutableListOf(IndexBlock(Index()))
        }

        val currentBlock = blocksForLevel.last()
        if (currentBlock.index.size >= INDEX_BLOCK_CAPACITY) {
            currentBlock.index.convertToHigherRank()
            blocksForLevel += IndexBlock(Index())
        }

        val block = blocksForLevel.last()

        // Assumes non-decreasing timestamps per block.
        if (domainLine.timestamp < block.maxTimestamp) return

        if (block.index.size == 0) {
            block.minSeq = domainLine.seq
        }

        val id = drainStore.put(domainLine)

        block.index.add(id, domainLine.toString())
        block.maxSeq = globalSeq.get()
        block.maxTimestamp = domainLine.timestamp

        size++
        State.indexedLines.addAndGet(1)
    }

    fun search(query: String, offsetLock: Long): Sequence<DomainLine> {
        val parsed = Query.parse(query)
        val lockedOffset = offsetLock - State.lock.get()

        return State.levels.get()
            .asSequence()
            .mapNotNull { level ->
                val blocks = levelIndexes[level] ?: return@mapNotNull null

                blocks.asReversed()
                    .asSequence()
                    .filter { it.minSeq < lockedOffset && it.minSeq < offsetLock }
                    .map { block ->
                        val ids: Sequence<Long> =
                            block.index.searchMustInclude(parsed.mustIncludeGroups) { true }

                        ids.mapNotNull { id ->
                            val line = drainStore.getLineBlocking(id) ?: return@mapNotNull null
                            if (line.seq <= lockedOffset &&
                                line.seq <= offsetLock &&
                                line.contains(parsed.mustInclude, parsed.mustNotInclude)
                            ) line else null
                        }
                    }
                    .reduceOrNull { acc, seq -> acc + seq }
            }.merge()
    }

    private data class Query(
        val mustNotInclude: List<String>,
        val mustInclude: List<String>,
        val mustIncludeGroups: List<List<String>>,
    ) {
        companion object {
            fun parse(raw: String): Query {
                val mustNot = mutableListOf<String>()
                val must = mutableListOf<String>()

                var inQuotes = false
                val phrase = StringBuilder()

                raw.split(' ')
                    .asSequence()
                    .map(String::trim)
                    .filter(String::isNotEmpty)
                    .forEach { token ->
                        when {
                            !inQuotes && token.startsWith("!") && token.length > 1 -> {
                                mustNot += token.drop(1)
                            }

                            !inQuotes && token.startsWith("\"") -> {
                                inQuotes = true
                                phrase.clear()
                                phrase.append(token.drop(1))

                                if (token.endsWith("\"") && token.length > 1) {
                                    val completed = phrase.dropLastIfEndsWithQuote()
                                    if (completed.isNotBlank()) must += completed
                                    inQuotes = false
                                }
                            }

                            inQuotes && token.endsWith("\"") -> {
                                phrase.append(' ').append(token)
                                val completed = phrase.dropLastIfEndsWithQuote()
                                if (completed.isNotBlank()) must += completed
                                inQuotes = false
                            }

                            inQuotes -> {
                                phrase.append(' ').append(token)
                            }

                            else -> must += token
                        }
                    }

                val filteredMust = must.filter(String::isNotBlank)
                return Query(
                    mustNotInclude = mustNot.filter(String::isNotBlank),
                    mustInclude = filteredMust,
                    mustIncludeGroups = listOf(filteredMust),
                )
            }

            private fun StringBuilder.dropLastIfEndsWithQuote(): String {
                val s = toString()
                return if (s.endsWith("\"")) s.dropLast(1) else s
            }
        }
    }
}