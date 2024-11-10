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
        version = it.status.containerStatuses.firstOrNull { it.name != "istio-proxy" && it.name != "daprd" }?.image?.substringAfterLast(":") ?: "",
        creationTimestamp = it.metadata.creationTimestamp
      )
    }
  }

  private fun getLogSequence(pod: String, namespace: String): LogWatch {
    val podResource = client.pods().inNamespace(namespace).withName(pod)
    return podResource.inContainer(podResource.get().spec.containers.first { it.name != "istio-proxy" }.name)
      .usingTimestamps().watchLog()
  }

  private fun getLogSequencePrev(pod: String, namespace: String): List<String> {
    val podResource = client.pods().inNamespace(namespace).withName(pod)
    return try {
      podResource.inContainer(podResource.get().spec.containers.first { it.name != "istio-proxy" }.name)
        .usingTimestamps().terminated().log.split("\n")
    } catch (e: Exception) {
      emptyList()
    }
  }

  @OptIn(DelicateCoroutinesApi::class)
  private fun addLogsToIndex(pod: String): AtomicBoolean {
    val threadDispatcher = newSingleThreadContext("CoroutineThread")
    val scope = CoroutineScope(threadDispatcher)

    val notStopping = AtomicBoolean(true)
    scope.launch {
      val podNamespacePair = client.pods().inAnyNamespace().list().items
        .firstOrNull { it.metadata.name == pod }?.metadata?.namespace ?: return@launch

      getLogSequencePrev(pod, podNamespacePair).forEach {
        Channels.popChannel.send(AddToIndex(it, pod))
      }

      while (notStopping.get()) {
        try {
          val logSequence = getLogSequence(pod, podNamespacePair)
          BufferedReader(InputStreamReader(logSequence.output)).use { reader ->
            while (notStopping.get()) {
              val line = reader.readLine()
              if (line != null) {
                Channels.popChannel.send(AddToIndex(line, pod))
              }
            }
          }
        } catch (e: Exception) {
          // Handle disconnection/reconnection here
          println("Lost connection for pod $pod. Retrying in 5 seconds...")
          delay(5000) // Wait for 5 seconds before retrying
        }
      }
    }
    return notStopping
  }
}

data class PodUnit(val name: String, val version: String, val creationTimestamp: String)
