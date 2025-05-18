package kube

import app.*
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import io.fabric8.kubernetes.client.dsl.LogWatch
import kotlinx.coroutines.*
import kotlinx.datetime.Instant
import kotlinx.serialization.json.Json
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicBoolean

class Kube {
    private val listenedPods: MutableMap<String, AtomicBoolean> = mutableMapOf()
    private val client = KubernetesClientBuilder().build()

    init {
        val thread = Thread {
            while (true) {
                when (val msg = Channels.podsChannel.take()) {
                    is ListPods -> msg.result.complete(listPodsInAllNamespaces())
                    is ListenToPod -> {
                        listenedPods[msg.podName] = addLogsToIndex(msg.podName)
                    }
                    is UnListenToPod -> {
                        listenedPods.remove(msg.podName)?.set(false)
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

        // Watch for new pods
        client.pods().inAnyNamespace().watch(object : Watcher<Pod> {
            override fun eventReceived(action: Watcher.Action, pod: Pod) {
                val name = pod.metadata.name
                if (action == Watcher.Action.ADDED && listenedPods.containsKey(name)) {
                    listenedPods[name] = addLogsToIndex(name)
                }
            }
            override fun onClose(p0: WatcherException?) {
            }
        })
    }

    fun listPodsInAllNamespaces(): List<PodUnit> {
        return client.pods().inAnyNamespace().list().items.map {
            PodUnit(
                name = it.metadata.name,
                version = it.status.containerStatuses.firstOrNull { it.name != "istio-proxy" && it.name != "daprd" }
                    ?.image?.substringAfterLast(":") ?: "",
                creationTimestamp = it.metadata.creationTimestamp
            )
        }
    }

    private fun getLogSequence(pod: String, namespace: String): LogWatch {
        val podResource = client.pods().inNamespace(namespace).withName(pod)
        val containerName = podResource.get().spec.containers.first { it.name != "istio-proxy" && it.name != "daprd" }.name
        return podResource.inContainer(containerName).usingTimestamps().watchLog()
    }

    private fun getLogSequencePrev(pod: String, namespace: String): List<String> {
        val podResource = client.pods().inNamespace(namespace).withName(pod)
        return try {
            val containerName = podResource.get().spec.containers.first { it.name != "istio-proxy" && it.name != "daprd" }.name
            podResource.inContainer(containerName).usingTimestamps().terminated().log.split("\n")
        } catch (e: Exception) {
            emptyList()
        }
    }

    @OptIn(DelicateCoroutinesApi::class, ExperimentalCoroutinesApi::class)
    private fun addLogsToIndex(pod: String): AtomicBoolean {
        val threadDispatcher = newSingleThreadContext("CoroutineThread-$pod")
        val scope = CoroutineScope(threadDispatcher)

        val notStopping = AtomicBoolean(true)
        scope.launch {
            val podNamespace = client.pods().inAnyNamespace().list().items
                .firstOrNull { it.metadata.name == pod }?.metadata?.namespace ?: return@launch

            getLogSequencePrev(pod, podNamespace).forEach {
                getLogJson(it, seq = seq.getAndAdd(1), indexIdentifier = pod)?.let {
                    Channels.popChannel.send(AddToIndexDomainLine(it))
                }
            }

            while (notStopping.get()) {
                try {
                    val logSequence = getLogSequence(pod, podNamespace)
                    BufferedReader(InputStreamReader(logSequence.output)).use { reader ->
                        while (notStopping.get()) {
                            val line = reader.readLine() ?: break
                            getLogJson(line, seq = seq.getAndAdd(1), indexIdentifier = pod)?.let {
                                Channels.popChannel.send(AddToIndexDomainLine(it))
                            }
                        }
                    }
                } catch (e: Exception) {
                    delay(2000)
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
