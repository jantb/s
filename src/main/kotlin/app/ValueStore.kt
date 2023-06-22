package app

import State
import State.searchTime
import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdDictCompress
import com.github.luben.zstd.ZstdDictTrainer
import deserializeJsonToObject
import index.Index
import serializeToJson
import java.io.Serializable
import java.time.OffsetDateTime
import java.util.*


val cap = 10_000

class ValueStore : Serializable {
    private val dictionaries = mutableListOf<ByteArray>()
    private val values = mutableListOf<Value>()
    private val indexes = mutableListOf(Index())
    var size = 0

    @Transient
    private var lru: Lru<Int, Domain>? = Lru(cap)

    @Transient
    private var lruContains: Lru<Int, Boolean>? = Lru(cap * 10)
    private var prevQuery = ""
    private val capacityLimitedList = CapacityLimitedList<Domain>(cap)
    fun put(key: Int, v: String) {
        val domain = try {
            v.deserializeJsonToObject<LogJson>()
        } catch (e: Exception) {
            try {
                LogEntry(OffsetDateTime.parse(v.substringBefore(" ")), "", v.substringAfter(" "))
            } catch (e: Exception) {
                LogEntry(OffsetDateTime.MIN, "", v)
            }
        }
        val value = domain.searchableString()



        size++
        if (indexes.last().size >= cap) {
            indexes.last().convertToHigherRank()
            indexes.add(Index())
        }
        State.indexedLines.addAndGet(1)
        indexes.last().add(key, value)

        if (!capacityLimitedList.add(domain)) {
            val clearAndGet = capacityLimitedList.clearAndGet()

            val trainer = ZstdDictTrainer(1024 * 1024, 64 * 1024) // 16 KB dictionary

            for (domain in clearAndGet) {
                trainer.addSample(domain.searchableString().toByteArray())
            }

            val dict = trainer.trainSamples()
            dictionaries.add(
                dict
            )
            val zstdDictCompress =
                ZstdDictCompress(dict, Zstd.defaultCompressionLevel())
            clearAndGet.forEach {
                val json = it.serializeToJson()
                values.add(
                    Value(
                        dictionaries.lastIndex,
                        Zstd.compress(json.toByteArray(), zstdDictCompress),
                        json.length
                    )
                )
            }
        }
    }

    fun search(query: String,length: Int): List<Domain> {

        if (lruContains == null) {
            lruContains = Lru(cap)
        }
        if (prevQuery != query) {
            lruContains!!.clear()
        }


        val queryList = query.split(" ").filter { !it.startsWith("!") }
        val queryListNot = query.split(" ").filter { it.startsWith("!") }.map { it.substring(1) }

        val nanoTime = System.nanoTime()

        val res = mutableListOf<Pair<Int, Domain>>()
        val time = System.currentTimeMillis()
        indexes.reversed().asSequence().takeWhile { System.currentTimeMillis() - time <= 50 }
            .map { index -> index.searchMustInclude(queryList) }.flatten()
            .map { it to get(it) }
            .filter { pair ->
                lruContains!![pair.first]?.let { return@filter it }
                val contains =
                    queryList.all { pair.second.searchableString().contains(it, true) } && queryListNot.none {
                        pair.second.searchableString().contains(
                            it,
                            true
                        )
                    }
                lruContains!![pair.first] = contains
                contains
            }
            .take(length)
            .forEach { res.add(it) }
        res.reverse()
        searchTime.set(System.nanoTime() - nanoTime)

        return res.map { it.second }
    }

    fun get(key: Int): Domain {
        if (lru == null) {
            lru = Lru(cap)
        }
        lru!![key]?.let { return it }

        val v = if (key < values.size) {
            val value = values[key]
            try {
                Zstd.decompress(value.bytes, dictionaries[value.dictionary], value.length)
                    .toString(Charsets.UTF_8).deserializeJsonToObject<LogEntry>()
            } catch (e: Exception) {
                Zstd.decompress(value.bytes, dictionaries[value.dictionary], value.length * 10)
                    .toString(Charsets.UTF_8).deserializeJsonToObject<LogEntry>()
            }
        } else {
            capacityLimitedList.get(key - values.size)?:LogEntry(OffsetDateTime.now(), "", "")
        }
        lru!![key] = v
        return v
    }
}

class Lru<K, V>(private val capacity: Int = Int.MAX_VALUE) {
    private val cache: LinkedHashMap<K, V> = LinkedHashMap(capacity, 0.75f, true)

    operator fun set(key: K, value: V) {
        cache[key] = value
        if (cache.size > capacity) {
            val iterator = cache.entries.iterator()
            iterator.next()
            iterator.remove()
        }
    }

    operator fun get(key: K): V? {
        return cache[key]
    }

    fun clear() {
        cache.clear()
    }
}

class CapacityLimitedList<T>(private val capacity: Int) : Serializable {
    private val list = mutableListOf<T>()
    fun add(element: T): Boolean {
        list.add(element)
        return list.size != capacity
    }

    fun clearAndGet(): List<T> {
        val toList = list.toList()
        list.clear()
        return toList
    }

    fun get(key: Int): T? {
        return list.getOrNull(key)
    }
}

class Value(val dictionary: Int, val bytes: ByteArray, val length: Int) : Serializable
