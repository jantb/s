package kube

import app.*
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.dsl.LogWatch
import kotlinx.coroutines.*
import kotlinx.datetime.Instant
import kotlinx.serialization.json.Json
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicBoolean

class Kube {
    private val listenedPods: MutableMap<String, AtomicBoolean> = mutableMapOf()

    init {
        val thread = Thread {
            while (true) {
                when (val msg = Channels.podsChannel.take()) {
                    is ListPods -> msg.result.complete(listPodsInAllNamespaces())
                    is ListenToPod -> {
                        listenedPods[msg.podName] = addLogsToIndex(msg.podName)
                    }

                    is UnListenToPod -> {
                        listenedPods.remove(msg.podName)!!.set(false)
                        // Channels.cmdChannel.put(ClearNamedIndex(msg.podName))
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

    val client = KubernetesClientBuilder().build()

    fun listPodsInAllNamespaces(): List<PodUnit> {
        return client.pods().inAnyNamespace().list().items.map {
            PodUnit(
                name = it.metadata.name,
                version = it.status.containerStatuses.firstOrNull { it.name != "istio-proxy" && it.name != "daprd" }?.image?.substringAfterLast(
                    ":"
                ) ?: "",
                creationTimestamp = it.metadata.creationTimestamp
            )
        }
    }

    private fun getLogSequence(pod: String, namespace: String): LogWatch {
        val podResource = client.pods().inNamespace(namespace).withName(pod)
        return podResource.inContainer(podResource.get().spec.containers.first { it.name != "istio-proxy" && it.name != "daprd" }.name)
            .usingTimestamps().watchLog()
    }

    private fun getLogSequencePrev(pod: String, namespace: String): List<String> {
        val podResource = client.pods().inNamespace(namespace).withName(pod)
        return try {
            podResource.inContainer(podResource.get().spec.containers.first { it.name != "istio-proxy" && it.name != "daprd" }.name)
                .usingTimestamps().terminated().log.split("\n")
        } catch (e: Exception) {
            emptyList()
        }
    }

    @OptIn(DelicateCoroutinesApi::class, ExperimentalCoroutinesApi::class)
    private fun addLogsToIndex(pod: String): AtomicBoolean {
        val threadDispatcher = newSingleThreadContext("CoroutineThread")
        val scope = CoroutineScope(threadDispatcher)

        val notStopping = AtomicBoolean(true)
        scope.launch {
            val podNamespacePair = client.pods().inAnyNamespace().list().items
                .firstOrNull { it.metadata.name == pod }?.metadata?.namespace ?: return@launch

            getLogSequencePrev(pod, podNamespacePair).forEach {
                getLogJson(it, seq = seq.getAndAdd(1), indexIdentifier = pod)?.let {
                    Channels.popChannel.send(AddToIndexDomainLine(it))
                }
            }

            while (notStopping.get()) {
                try {
                    val logSequence = getLogSequence(pod, podNamespacePair)
                    BufferedReader(InputStreamReader(logSequence.output)).use { reader ->
                        while (notStopping.get()) {
                            val line = reader.readLine()
                            if (line.isNullOrBlank()) {
                                notStopping.set(false)
                            }
                            getLogJson(line, seq = seq.getAndAdd(1), indexIdentifier = pod)?.let {
                                Channels.popChannel.send(AddToIndexDomainLine(it))
                            }

                        }
                    }
                } catch (e: Exception) {
                    notStopping.set(false)
                }
            }
        }
        return notStopping
    }
}
private val json = Json { ignoreUnknownKeys = true }

private fun getLogJson(v: String, seq: Long, indexIdentifier: String): DomainLine? {
    val index = v.indexOf(' ')
    val timestamp = if (index != -1) v.substring(0, index) else v
    val message = if (index != -1) v.substring(index + 1) else ""
    return try {
        val ecsDocument = json.decodeFromString<EcsDocument>(message)
        LogLineDomain(seq = seq, indexIdentifier = indexIdentifier, ecsDocument = ecsDocument)
    } catch (e: Exception) {
        runCatching {
            LogLineDomain(
                seq,
                timestamp = Instant.parse(timestamp).toEpochMilliseconds(),
                level = LogLevel.UNKNOWN,
                threadName = "",
                serviceName = "",
                serviceVersion = "",
                logger = "",
                message = message,
                indexIdentifier = indexIdentifier,
            )
        }.getOrNull()
    }
}

data class PodUnit(val name: String, val version: String, val creationTimestamp: String)
