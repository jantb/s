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
import merge
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.LinkedBlockingDeque
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class App : CoroutineScope {
    override val coroutineContext: CoroutineContext = Dispatchers.IO

    fun start() {
        // Use ConcurrentHashMap for thread-safe valueStores
        val valueStores = ConcurrentHashMap<String, ValueStore>()
        // Use AtomicLong for thread-safe sequence counter
        val seq = AtomicLong(0L)
        // Use AtomicLong for thread-safe offsetLock
        val offsetLock = AtomicLong(Long.MAX_VALUE)

        // Launch multiple worker coroutines to process messages concurrently
        repeat(Runtime.getRuntime().availableProcessors()) { workerId -> // Adjust the number of workers based on your needs
            launch(CoroutineName("indexUpdaterWorker-$workerId")) {
                while (true) {
                    // Receive messages from either channel
                    val msg = select {
                        searchChannel.onReceive { it }
                        popChannel.onReceive { it }
                    }

                    when (msg) {
                        is AddToIndex -> {
                            // Process AddToIndex sequentially to maintain seq order
                            synchronized(valueStores) {
                                val sequence = seq.incrementAndGet()
                                valueStores.computeIfAbsent(msg.indexIdentifier) { ValueStore() }
                                    .put(sequence, msg.value, msg.indexIdentifier)
                                changedAt.set(System.nanoTime())
                            }
                        }

                        is ClearIndex -> {
                            synchronized(valueStores) {
                                valueStores.clear()
                                indexedLines.set(0)
                            }
                        }

                        is ClearNamedIndex -> {
                            synchronized(valueStores) {
                                val remove = valueStores.remove(msg.name)
                                if (remove != null) {
                                    indexedLines.addAndGet(-remove.size)
                                }
                            }
                        }

                        is QueryChanged -> {
                            // Update offsetLock atomically
                            if (msg.offset > 0) {
                                offsetLock.compareAndSet(Long.MAX_VALUE, seq.get())
                            } else {
                                offsetLock.set(Long.MAX_VALUE)
                            }

                            // Perform search concurrently (Index.searchMustInclude is thread-safe)
                            val listResults = measureTimedValue {
                                valueStores.map {
                                    it.value.search(
                                        query = msg.query,
                                        length = msg.length + msg.offset,
                                        offsetLock = offsetLock.get(),
                                        levels = msg.levels
                                    ).asSequence()
                                }.merge(descending = true).drop(msg.offset).take(msg.length).toList().reversed()
                            }

                            val chartResults = measureTimedValue {
                                valueStores.map {
                                    it.value.search(
                                        query = msg.query,
                                        length = msg.length + msg.offset + 10_000,
                                        offsetLock = offsetLock.get(),
                                        levels = msg.levels
                                    ).asSequence()
                                }.merge(descending = true).drop(msg.offset).take(msg.length + 10_000).toList()
                            }

                            // Update searchTime atomically
                            searchTime.set(listResults.duration.inWholeNanoseconds + chartResults.duration.inWholeNanoseconds)

                            // Send results to the UI
                            cmdGuiChannel.put(ResultChanged(listResults.value, chartResults.value))
                        }
                    }
                }
            }
        }
    }
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
class QueryChanged(val query: String, val length: Int, val offset: Int, val levels: Set<String>? = null) : CmdMessage()
object ClearIndex : CmdMessage()
class ClearNamedIndex(val name: String) : CmdMessage()
class AddToIndex(val value: String, val indexIdentifier: String = UUID.randomUUID().toString()) : CmdMessage()

sealed class CmdGuiMessage
class ResultChanged(val result: List<Domain>, val chartResult: List<Domain> = emptyList()) : CmdGuiMessage()
class KafkaLagInfo(val lagInfo: List<kafka.Kafka.LagInfo>) : CmdGuiMessage()