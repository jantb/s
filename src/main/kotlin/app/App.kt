@file:OptIn(ExperimentalTime::class)

package app

import LogCluster
import LogLevel
import State.changedAt
import State.indexedLines
import State.searchTime
import app.Channels.kafkaCmdGuiChannel
import app.Channels.logClusterCmdGuiChannel
import app.Channels.popChannel
import app.Channels.refreshChannel
import app.Channels.searchChannel
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kube.PodUnit
import merge
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingDeque
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class App : CoroutineScope {
    fun start() {

        launch(CoroutineName("indexUpdaterScheduler")) {
            val valueStores = mutableMapOf<String, ValueStore>()
            var seq = 0L
            var offsetLock = 0L
            while (true) {
                when (val msg = select {
                    searchChannel.onReceive { it }
                    popChannel.onReceive { it }
                    refreshChannel.onReceive { it }
                }) {
                    is AddToIndex -> {
                        seq++
                        valueStores.computeIfAbsent(msg.indexIdentifier) { ValueStore() }
                            .put(seq, msg.value, msg.indexIdentifier, msg.app)
                        changedAt.set(System.nanoTime())
                    }

                    is ClearIndex -> {
                        valueStores.clear()
                    }

                    is ClearNamedIndex -> {
                        val remove = valueStores.remove(msg.name)
                        if (remove != null) {
                            indexedLines.addAndGet(-remove.size)
                        }
                    }
                    is RefreshLogGroups ->{
                        logClusterCmdGuiChannel.put(LogClusterList(valueStores.values.map { it.getLogClusters() }.flatten()))
                    }

                    is QueryChanged -> {
                        if (msg.offset > 0) {
                            if (offsetLock == Long.MAX_VALUE) {
                                offsetLock = seq
                            }
                        } else {
                            offsetLock = Long.MAX_VALUE
                        }

                        val listResults = measureTimedValue {
                            valueStores.map {
                                it.value.search(
                                    query = msg.query,
                                    length = msg.length + msg.offset,
                                    offsetLock = offsetLock
                                ).asSequence()
                            }.merge(descending = true).drop(msg.offset).take(msg.length).toList().reversed()
                        }

                        val chartResults =
                                valueStores.map {
                                    it.value.search(
                                        query = msg.query,
                                        length = msg.length + msg.offset + 10_000,
                                        offsetLock = offsetLock
                                    ).asSequence()
                                }.merge(descending = true).drop(msg.offset).take(msg.length + 10_000).toList()

                        searchTime.set(listResults.duration.inWholeNanoseconds)

                        kafkaCmdGuiChannel.put(ResultChanged(listResults.value, chartResults))
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
    val refreshChannel = Channel<CmdMessage>(1)
}

sealed class PodsMessage
sealed class KafkaMessage
sealed class KafkaSelectMessage
class ListPods(val result: CompletableFuture<List<PodUnit>> = CompletableFuture()) : PodsMessage()
class ListTopics(val result: CompletableFuture<List<String>> = CompletableFuture()) : KafkaMessage()
data object ListLag : KafkaMessage()
class ListenToPod(val podName: String) : PodsMessage()
class PublishToTopic(val topic: String, val key: String, val value: String) : KafkaMessage()
class ListenToTopic(val name: List<String>) : KafkaMessage()
class UnListenToPod(val podName: String) : PodsMessage()
object UnListenToPods : PodsMessage()
class KafkaSelectChangedText(val text: String) : KafkaSelectMessage()
object UnListenToTopics : KafkaMessage()
sealed class CmdMessage
class QueryChanged(val query: String, val length: Int, val offset: Int, val levels: Set<LogLevel>) : CmdMessage()
object ClearIndex : CmdMessage()

class ClearNamedIndex(val name: String) : CmdMessage()

class AddToIndex(val value: String, val indexIdentifier: String = UUID.randomUUID().toString(), val app: Boolean) : CmdMessage()
data object RefreshLogGroups : CmdMessage()

sealed class CmdGuiMessage
class ResultChanged(val result: List<Domain>, val chartResult: List<Domain> = emptyList()) : CmdGuiMessage()
class KafkaLagInfo(val lagInfo: List<kafka.Kafka.LagInfo>) : CmdGuiMessage()
class LogClusterList(val clusters: List<LogCluster>) : CmdGuiMessage()

