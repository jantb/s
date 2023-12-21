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

  fun listPodsInNamespace(): List<PodUnit> {
    return client.pods().list().items.map {
      PodUnit(
        name = it.metadata.name,
        version = it.status.containerStatuses.firstOrNull { it.name != "istio-proxy" }?.image?.substringAfterLast(
          ":"
        ) ?: "",
        creationTimestamp = it.metadata.creationTimestamp
      )
    }
  }


  private fun getLogSequence(pod: String): LogWatch {
    val podResource = client.pods().inNamespace(client.pods().list().items.filter { it.metadata.name == pod }
      .firstNotNullOfOrNull { it.metadata.namespace }).withName(pod)

    return podResource.inContainer(podResource.get().spec.containers.first { it.name != "istio-proxy" }.name)
      .usingTimestamps().watchLog()
  }

  private fun getLogSequencePrev(pod: String): List<String> {
    val podResource = client.pods().inNamespace(client.pods().list().items.filter { it.metadata.name == pod }
      .map { it.metadata.namespace }.firstOrNull()).withName(pod)

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
      getLogSequencePrev(pod).forEach {
        Channels.popChannel.send(AddToIndex(it, pod))
      }

      val logSequence = getLogSequence(pod)
      BufferedReader(InputStreamReader(logSequence.output)).use { reader ->
        while (notStopping.get()) {
          val line = reader.readLine()
          if (line != null) {
            Channels.popChannel.send(AddToIndex(line, pod))
          }
        }
      }
    }
    return notStopping
  }
}

data class PodUnit(val name: String, val version: String, val creationTimestamp: String)
