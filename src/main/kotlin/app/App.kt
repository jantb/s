package app

import State.changedAt
import State.indexedLines
import app.Channels.cmdChannel
import app.Channels.cmdGuiChannel
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingDeque
import kotlin.system.measureTimeMillis

class App {

    fun start() {
        val thread = Thread {
            val valueStores = mutableMapOf<String, ValueStore>()
            var query = ""
            var time = System.currentTimeMillis()
            var lastQueryTime = 0L
            var length = 0
            var offset = 0
            while (true) {
                try {
                    when (val msg = cmdChannel.take()) {
                        is QueryChanged -> {
                            query = msg.query
                            val timeSinceLast = System.currentTimeMillis() - time
                            if (timeSinceLast > 16 && timeSinceLast > lastQueryTime) {
                                cmdGuiChannel.offer(ResultChanged(result =
                                    valueStores.map { it.value.search(query, length + offset) }
                                        .flatten().sortedBy { it.timestamp() }.takeLast(length + offset)
                                        .dropLast(offset),query =query))
                                time = System.currentTimeMillis()
                            }
                        }

                        is UpdateResult -> {
                            lastQueryTime = measureTimeMillis {
                                length = msg.length
                                offset = msg.offset
                            }
                            cmdGuiChannel.offer(ResultChanged(result =
                            valueStores.map { it.value.search(query, length + offset) }
                                .flatten().sortedBy { it.timestamp() }.takeLast(length + offset)
                                .dropLast(offset),query =query))
                            time = System.currentTimeMillis()
                        }

                        is AddToIndex -> {
                            valueStores.computeIfAbsent(msg.indexIdentifier) { ValueStore() }.put(msg.key, msg.value)
                            changedAt.set(System.nanoTime())
                        }

                        is ClearIndex -> {
                            valueStores.clear()
                            indexedLines.set(0)
                        }

                        is ClearNamedIndex -> {
                            indexedLines.addAndGet( -valueStores.remove(msg.name)!!.size)}
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
        thread.isDaemon = true
        thread.start()
    }
}

object Channels {
    val cmdChannel = LinkedBlockingDeque<CmdMessage>(1)
    val cmdGuiChannel = LinkedBlockingDeque<CmdGuiMessage>(1)
    val podsChannel = LinkedBlockingDeque<PodsMessage>(1)
}

sealed class PodsMessage
class ListPods(val result: CompletableFuture<List<String>> = CompletableFuture()) : PodsMessage()
class ListenToPod( val podName: String) : PodsMessage()
class UnListenToPod(val podName: String) : PodsMessage()
object UnListenToPods : PodsMessage()
sealed class CmdMessage
class QueryChanged(val query: String) : CmdMessage()
object ClearIndex : CmdMessage()

class ClearNamedIndex(val name: String) : CmdMessage()


class AddToIndex(val key: Int, val value: String, val indexIdentifier: String = UUID.randomUUID().toString()) :
    CmdMessage()

class UpdateResult(
    val offset: Int,
    val length: Int
) :
    CmdMessage()

sealed class CmdGuiMessage
class ResultChanged(val result: List<Domain>, val query: String ) : CmdGuiMessage()
