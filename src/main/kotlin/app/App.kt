
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
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingDeque
import kotlin.coroutines.CoroutineContext
import kotlin.time.measureTimedValue
class App : CoroutineScope {
    fun start() {
        launch(CoroutineName("indexUpdaterScheduler")) {
            val valueStores = mutableMapOf<String, ValueStore>()
            var offsetLock = 0L

            // Define a class to store our virtual buffer
            data class VirtualBuffer(
                val data: List<DomainLine>,
                val query: String,
                val offsetLock: Long,
                val totalSize: Int,
                val bufferStartOffset: Int // Starting offset of the buffer
            )

            // Create a variable to hold our virtual buffer
            var virtualBuffer: VirtualBuffer? = null
            val bufferSize = 40000 // Size of our virtual buffer
            val fetchSize = 5000 // Number of new items to fetch

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

                        // Invalidate virtual buffer as data has changed
                        virtualBuffer = null
                    }

                    is ClearNamedIndex -> {
                        val remove = valueStores.remove(msg.name)
                        if (remove != null) {
                            indexedLines.addAndGet(-remove.size)
                        }

                        // Invalidate virtual buffer as data has changed
                        virtualBuffer = null
                    }

                    is RefreshLogGroups -> {
                        logClusterCmdGuiChannel.put(LogClusterList(valueStores.values.map { it.getLogClusters() }.flatten()))
                    }

                    is QueryChanged -> {
                        if (msg.offset > 0) {
                            if (offsetLock == Long.MAX_VALUE) {
                                offsetLock = seq.get()
                            }
                        } else {
                            offsetLock = Long.MAX_VALUE
                            // Clear virtual buffer for new searches (offset = 0)
                            virtualBuffer = null
                        }

                        // Check if we can use the virtual buffer
                        val canUseBuffer = virtualBuffer != null &&
                                msg.query == virtualBuffer.query &&
                                msg.offset >= virtualBuffer.bufferStartOffset &&
                                msg.offset < virtualBuffer.bufferStartOffset + virtualBuffer.totalSize

                        val listResults = measureTimedValue {
                            if (canUseBuffer) {
                                // Use data from buffer
                                val bufferOffset = msg.offset - virtualBuffer!!.bufferStartOffset
                                virtualBuffer!!.data.drop(bufferOffset).take(msg.length).toList()
                            } else {
                                // Determine if we need to fetch new data
                                val needNewSearch = virtualBuffer == null ||
                                        msg.query != virtualBuffer!!.query ||
                                        offsetLock != virtualBuffer!!.offsetLock ||
                                        msg.offset < virtualBuffer!!.bufferStartOffset ||
                                        msg.offset >= virtualBuffer!!.bufferStartOffset + virtualBuffer!!.totalSize

                                if (needNewSearch) {
                                    // Handle scrolling or new search
                                    val (fetchOffset, newBufferStartOffset) = when {
                                        // Scrolling forward: fetch new data from buffer end
                                        virtualBuffer != null && msg.offset >= virtualBuffer!!.bufferStartOffset + virtualBuffer!!.totalSize -> {
                                            val newOffset = virtualBuffer!!.bufferStartOffset + virtualBuffer!!.totalSize
                                            newOffset to (virtualBuffer!!.bufferStartOffset + virtualBuffer!!.totalSize - virtualBuffer!!.data.size + fetchSize)
                                        }
                                        // Scrolling backward: fetch data before buffer start
                                        virtualBuffer != null && msg.offset < virtualBuffer!!.bufferStartOffset -> {
                                            val newOffset = maxOf(0, virtualBuffer!!.bufferStartOffset - fetchSize)
                                            newOffset to newOffset
                                        }
                                        // New search or buffer invalidated
                                        else -> {
                                            msg.offset to msg.offset
                                        }
                                    }

                                    // Fetch new data (limited to fetchSize)
                                    val newResults = valueStores.map {
                                        it.value.search(
                                            query = msg.query,
                                            offsetLock = offsetLock
                                        )
                                    }.merge().drop(fetchOffset).take(fetchSize).toList()

                                    // Update buffer: combine with existing data and prune
                                    val updatedData = if (virtualBuffer != null && msg.offset >= virtualBuffer!!.bufferStartOffset) {
                                        // Scrolling forward: append new data, prune from start
                                        (virtualBuffer!!.data + newResults).takeLast(bufferSize)
                                    } else if (virtualBuffer != null && msg.offset < virtualBuffer!!.bufferStartOffset) {
                                        // Scrolling backward: prepend new data, prune from end
                                        (newResults + virtualBuffer!!.data).take(bufferSize)
                                    } else {
                                        // New search: use new results, up to bufferSize
                                        newResults.take(bufferSize)
                                    }

                                    // Update virtual buffer
                                    virtualBuffer = VirtualBuffer(
                                        data = updatedData,
                                        query = msg.query,
                                        offsetLock = offsetLock,
                                        totalSize = updatedData.size,
                                        bufferStartOffset = newBufferStartOffset
                                    )
                                }

                                // Return the requested portion from the buffer
                                val bufferOffset = msg.offset - virtualBuffer!!.bufferStartOffset
                                virtualBuffer!!.data.drop(bufferOffset).take(msg.length).toList()
                            }
                        }

                        searchTime.set(listResults.duration.inWholeNanoseconds)

                        // Send results to the UI
                        kafkaCmdGuiChannel.put(ResultChanged(
                            listResults.value.reversed(),
                            virtualBuffer?.data?.drop(maxOf(0, msg.offset - virtualBuffer!!.bufferStartOffset))?.take(30000) ?: listResults.value
                        ))
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