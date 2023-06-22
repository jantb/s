@file:OptIn(FlowPreview::class)

package kube

import app.*
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.dsl.LogWatch
import kotlinx.coroutines.*
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicBoolean


class Kube {
    private val listenedPods: MutableMap<String, AtomicBoolean> = mutableMapOf()

    init {
        val thread = Thread {
            while (true) {
                when (val msg = Channels.podsChannel.take()) {
                    is ListNamespaces -> {
                        msg.result.complete(listNamespaces())
                    }

                    is ListPods -> msg.result.complete(listPodsInNamespace(msg.nameSpace))
                    is ListenToPod -> {
                        listenedPods[msg.podName] = addLogsToIndex(msg.nameSpace, msg.podName)
                    }

                    is UnListenToPod -> {
                        listenedPods.remove(msg.podName)!!.set(true)
                        Channels.cmdChannel.put(ClearNamedIndex(msg.podName))
                    }

                    is UnListenToPods -> {
                        listenedPods.forEach { it.value.set(true) }
                        listenedPods.clear()
                    }
                }
            }
        }
        thread.isDaemon = true
        thread.start()
    }

    val readers = mutableListOf<LogWatch>()
    val client = KubernetesClientBuilder().build()
    fun listNamespaces(): List<String> {
        return client.namespaces().list().items.map { it.metadata.name }
    }

    fun listPodsInNamespace(namespace: String): List<String> {
        return client.pods().inNamespace(namespace).list().items.map { it.metadata.name }
    }

    fun clearReaders() {
        readers.forEach { it.close() }
        readers.clear()
    }

    fun getLogSequence(namespace: String, pod: String): LogWatch {
        return client.pods().inNamespace(namespace).withName(pod).usingTimestamps().watchLog()
    }

    @OptIn(DelicateCoroutinesApi::class)
    private fun addLogsToIndex(nameSpace: String, pod: String): AtomicBoolean {
        val threadDispatcher = newSingleThreadContext("CoroutineThread")
        val scope = CoroutineScope(threadDispatcher)

        val notStopping = AtomicBoolean(true)
        scope.launch {
            val logSequence = getLogSequence(nameSpace, pod)
            BufferedReader(InputStreamReader(logSequence.output)).use { reader ->
                var docNr = 0
                while (notStopping.get()) {
                    withTimeoutOrNull(100) {
                        try {
                            val line = reader.readLine()
                            if (line != null) {
                                Channels.cmdChannel.put(AddToIndex(docNr++, line, pod))
                            }

                        } catch (e: Exception) {
                            notStopping.set(false)
                            return@withTimeoutOrNull
                        }
                    }
                }
            }
        }
        return notStopping
    }
}
