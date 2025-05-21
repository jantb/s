package app

import LogCluster
import State.changedAt
import State.indexedLines
import State.searchTime
import app.Channels.kafkaCmdGuiChannel
import app.Channels.logClusterCmdGuiChannel
import app.Channels.popChannel
import app.Channels.refreshChannel
import app.Channels.searchChannel
import kafka.Kafka
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.selects.select
import kube.PodUnit
import merge
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingDeque
import kotlin.coroutines.CoroutineContext
import kotlin.time.measureTimedValue

class App : CoroutineScope {

    private class CachedList {
        private var query = UUID.randomUUID().toString()
        private var queryChanged = true
        private var results = emptyList<DomainLine>()
        private var resultsOffsetStart = 0L
        private var complete = false
        fun setQuery(newQuery: String) {
            queryChanged = query != newQuery
            query = newQuery
        }

        fun setResults(results: List<DomainLine>, offset: Long, complete: Boolean) {
            this.results = results
            this.resultsOffsetStart = offset - kotlin.math.min(offset, 5000)
            this.complete = complete
        }

        fun needRefresh(offset: Long): Boolean {
            return queryChanged ||
                    resultsOffsetStart > offset ||
                    ((offset - resultsOffsetStart + 5000) > results.size ) && !complete
        }

        fun get(offset: Long): List<DomainLine> {
            return results.drop((offset - resultsOffsetStart).toInt())
        }
    }


    fun start() {
        launch(CoroutineName("indexUpdaterScheduler")) {
            val valueStores = mutableMapOf<String, ValueStore>()
            var offsetLock = 0L
            val buffer = CachedList()
            while (true) {
                when (val msg = select {
                    searchChannel.onReceive { it }
                    popChannel.onReceive { it }
                    refreshChannel.onReceive { it }
                }) {

                    is AddToIndexDomainLine -> {
                        valueStores.computeIfAbsent(msg.domainLine.indexIdentifier) { ValueStore() }
                            .put(msg.domainLine)
                        changedAt.set(System.nanoTime())
                    }

                    is ClearNamedIndex -> {
                        val remove = valueStores.remove(msg.name)
                        if (remove != null) {
                            indexedLines.addAndGet(-remove.size)
                        }
                    }

                    is RefreshLogGroups -> {
                        logClusterCmdGuiChannel.put(LogClusterList(valueStores.values.map { it.getLogClusters() }
                            .flatten()))
                    }

                    is QueryChanged -> {


                        if (msg.offset > 0) {
                            if (offsetLock == Long.MAX_VALUE) {
                                offsetLock = seq.get()
                            }
                        } else {
                            offsetLock = Long.MAX_VALUE
                        }

                        val listResults = measureTimedValue {
                            val n = 10000
                            if (msg.offset > 0) {
                                buffer.setQuery(newQuery = msg.query)
                                if (buffer.needRefresh(offset = msg.offset.toLong())) {
                                    val cacheStartOffset = kotlin.math.max(0, msg.offset - 5000)
                                    val results = valueStores.map {
                                        it.value.search(
                                            query = msg.query,
                                            offsetLock = offsetLock
                                        )
                                    }.merge().drop(cacheStartOffset).take(n + 10000).toList()
                                    buffer.setResults(results, cacheStartOffset.toLong(), results.size != n + 10000 )
                                }

                                buffer.get(msg.offset.toLong()).take(n)

                            } else {
                                valueStores.map {
                                    it.value.search(
                                        query = msg.query,
                                        offsetLock = offsetLock
                                    )
                                }.merge().drop(msg.offset).take(n).toList()
                            }
                        }

                        searchTime.set(listResults.duration.inWholeNanoseconds)

                        kafkaCmdGuiChannel.put(
                            ResultChanged(
                                listResults.value.take(msg.length).reversed(),
                                listResults.value
                            )
                        )
                    }
                }
            }
        }
    }


    override val coroutineContext: CoroutineContext = Dispatchers.IO
}

object Channels {
    val kafkaCmdGuiChannel = LinkedBlockingDeque<CmdGuiMessage>(1)
    val logClusterCmdGuiChannel = LinkedBlockingDeque<CmdGuiMessage>(1)
    val podsChannel = LinkedBlockingDeque<PodsMessage>(1)
    val kafkaChannel = LinkedBlockingDeque<KafkaMessage>(1)
    val kafkaSelectChannel = LinkedBlockingDeque<KafkaSelectMessage>(1)
    val popChannel = Channel<CmdMessage>(capacity = BUFFERED)
    val searchChannel = Channel<QueryChanged>(capacity = CONFLATED)
    val refreshChannel = Channel<CmdMessage>(CONFLATED)
}

sealed class PodsMessage
sealed class KafkaMessage
sealed class KafkaSelectMessage
class ListPods(val result: CompletableFuture<List<PodUnit>> = CompletableFuture()) : PodsMessage()
class ListTopics(val result: CompletableFuture<List<String>> = CompletableFuture()) : KafkaMessage()
data class ListLag(val result: CompletableDeferred<List<Kafka.LagInfo>> = CompletableDeferred()) : KafkaMessage()
class ListenToPod(val podName: String) : PodsMessage()
class PublishToTopic(val topic: String, val key: String, val value: String) : KafkaMessage()
class ListenToTopic(val name: List<String>) : KafkaMessage()
class UnListenToPod(val podName: String) : PodsMessage()
object UnListenToPods : PodsMessage()
class KafkaSelectChangedText(val text: String) : KafkaSelectMessage()
object UnListenToTopics : KafkaMessage()
sealed class CmdMessage
class QueryChanged(val query: String, val length: Int, val offset: Int) : CmdMessage()

class ClearNamedIndex(val name: String) : CmdMessage()

class AddToIndexDomainLine(val domainLine: DomainLine) : CmdMessage()
data object RefreshLogGroups : CmdMessage()

sealed class CmdGuiMessage
class ResultChanged(val result: List<DomainLine>, val chartResult: List<DomainLine> = emptyList()) : CmdGuiMessage()
class KafkaLagInfo(val lagInfo: List<kafka.Kafka.LagInfo>) : CmdGuiMessage()
class LogClusterList(val clusters: List<LogCluster>) : CmdGuiMessage()

