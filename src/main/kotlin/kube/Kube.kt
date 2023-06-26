@file:OptIn(FlowPreview::class)

package kube

import app.*
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.dsl.LogWatch
import io.fabric8.kubernetes.client.utils.internal.PodOperationUtil.watchLog
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

                    is ListPods -> msg.result.complete(listPodsInNamespace())
                    is ListenToPod -> {
                        listenedPods[msg.podName] = addLogsToIndex(msg.podName)
                    }

                    is UnListenToPod -> {
                        listenedPods.remove(msg.podName)!!.set(false)
                        Channels.cmdChannel.put(ClearNamedIndex(msg.podName))
                    }

                    is UnListenToPods -> {
                        listenedPods.forEach { it.value.set(false) }
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

    fun listPodsInNamespace(): List<String> {
        return client.pods().list().items.map { it.metadata.name }
    }

    fun clearReaders() {
        readers.forEach { it.close() }
        readers.clear()
    }

    private fun getLogSequence(pod: String): LogWatch {
        val podResource = client.pods().inNamespace(client.pods().list().items.filter { it.metadata.name == pod }
            .map { it.metadata.namespace }.first()).withName(pod)

        return podResource.inContainer(podResource.get().spec.containers.first { it.name != "istio-proxy" }.name).usingTimestamps().watchLog()
    }

    private fun getLogSequencePrev(pod: String): List<String> {
        val podResource = client.pods().inNamespace(client.pods().list().items.filter { it.metadata.name == pod }
            .map { it.metadata.namespace }.first()).withName(pod)

        return try {
            podResource.inContainer(podResource.get().spec.containers.first { it.name != "istio-proxy" }.name)
                .usingTimestamps().terminated().log.split("\n")
        } catch (e: Exception) {
            emptyList()
        }
    }

    @OptIn(DelicateCoroutinesApi::class)
    private fun addLogsToIndex( pod: String): AtomicBoolean {
        val threadDispatcher = newSingleThreadContext("CoroutineThread")
        val scope = CoroutineScope(threadDispatcher)

        val notStopping = AtomicBoolean(true)
        scope.launch {
            getLogSequencePrev(pod).forEach {
                Channels.cmdChannel.put(AddToIndex(it, pod))
            }

            val logSequence = getLogSequence( pod)
            BufferedReader(InputStreamReader(logSequence.output)).use { reader ->
                while (notStopping.get()) {
                    withTimeoutOrNull(100) {
                        try {
                            val line = reader.readLine()
                            if (line != null) {
                                Channels.cmdChannel.put(AddToIndex(line, pod))
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
