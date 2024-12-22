@file:OptIn(ExperimentalTime::class)

package app

import State.changedAt
import State.indexedLines
import State.searchTime
import app.Channels.cmdGuiChannel
import app.Channels.popChannel
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
                }) {
                    is AddToIndex -> {
                        seq++
                        valueStores.computeIfAbsent(msg.indexIdentifier) { ValueStore() }
                            .put(seq, msg.value, msg.indexIdentifier)
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

                    is QueryChanged -> {
                        if (msg.offset > 0) {
                            if (offsetLock == Long.MAX_VALUE) {
                                offsetLock = seq
                            }
                        } else {
                            offsetLock = Long.MAX_VALUE
                        }
                        val results = measureTimedValue {
                            valueStores.map { it.value.search(
                                query = msg.query,
                                length = msg.length + msg.offset,
                                offsetLock = offsetLock
                            ) }
                                .flatten().sortedBy { it.timestamp() }.dropLast(msg.offset).takeLast(msg.length)
                        }
                        searchTime.set(results.duration.inWholeNanoseconds)
                        cmdGuiChannel.put(ResultChanged(results.value))
                    }
                }
            }
        }
    }


    override val coroutineContext: CoroutineContext = Dispatchers.IO
}

object Channels {
    val cmdGuiChannel = LinkedBlockingDeque<CmdGuiMessage>(1)
    val podsChannel = LinkedBlockingDeque<PodsMessage>(1)
    val kafkaChannel = LinkedBlockingDeque<KafkaMessage>(1)
    val kafkaSelectChannel = LinkedBlockingDeque<KafkaSelectMessage>(1)
    val popChannel = Channel<CmdMessage>(capacity = BUFFERED)
    val searchChannel = Channel<QueryChanged>(capacity = CONFLATED)
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
class QueryChanged(val query: String, val length: Int, val offset: Int) : CmdMessage()
object ClearIndex : CmdMessage()

class ClearNamedIndex(val name: String) : CmdMessage()

class AddToIndex(val value: String, val indexIdentifier: String = UUID.randomUUID().toString()) :
    CmdMessage()

sealed class CmdGuiMessage
class ResultChanged(val result: List<Domain>) : CmdGuiMessage()
