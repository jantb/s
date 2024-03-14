package app

import State
import deserializeJsonToObject
import util.CountMin
import util.Index
import java.io.Serializable
import java.time.OffsetDateTime


val cap = 100_000

class ValueStore : Serializable {
  private val indexes = mutableListOf(Index<LogJson>())
  private val heavyHitters = CountMin()
  var size = 0

  fun put(v: String, indexIdentifier: String) {
    val domain = try {
      val logJson = v.substringAfter(" ").deserializeJsonToObject<LogJson>()
      logJson.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant()
      logJson.init()
      logJson
    } catch (e: Exception) {
      val logJson = LogJson(message = v.substringAfter(" "), application = indexIdentifier)
      try {
        logJson.timestamp = OffsetDateTime.parse(v.substringBefore(" ")).toInstant()
        logJson.init()
      } catch (e: Exception) {
        return
      }
      logJson
    }
    val value = domain.searchableString()

    size++
    if (indexes.last().size >= cap) {
      indexes.last().convertToHigherRank()
      indexes.add(Index())
    }
    State.indexedLines.addAndGet(1)
    heavyHitters.add(domain.getPunct())
    indexes.last().add(domain, value)
  }

  fun search(query: String, length: Int): List<Domain> {

    val queryList = mutableListOf<String>()
    val queryListNot = mutableListOf<String>()
    var isInPhrase = false
    var phrase = ""

    query.split(" ").forEach { word ->
      if (word.startsWith("!") && !isInPhrase) {
        queryListNot.add(word.substring(1))
      } else if (word.startsWith("\"") && !isInPhrase) {
        isInPhrase = true
        phrase = word.substring(1)
      } else if (word.endsWith("\"") && isInPhrase) {
        phrase += " $word"
        phrase = phrase.substring(0, phrase.length - 1)
        queryList.add(phrase)
        isInPhrase = false
      } else if (isInPhrase) {
        phrase += " $word"
      } else {
        queryList.add(word)
      }
    }


    return indexes.reversed().asSequence()
      .map { index -> index.searchMustInclude(listOf(queryList.filter { it.isNotBlank() }) )}.flatten()
      .filter { domain ->
        val contains =
          if (State.heavyHitters.get() && heavyHitters.topKSetAbove(domain.getPunct())) {
            false
          } else if (queryList.isEmpty() && queryListNot.isEmpty()) {
            true
          } else if (queryList.isEmpty()) {
            queryListNot.none {
              domain.searchableString().contains(
                it,
                true
              )
            }
          } else if (queryListNot.isEmpty()) {
            queryList.all { domain.searchableString().contains(it, true) }
          } else {
            queryList.all { domain.searchableString().contains(it, true) } && queryListNot.none {
              domain.searchableString().contains(
                it,
                true
              )
            }
          }

        contains
      }.sortedByDescending { it.timestamp }.take(length)
      .toList().reversed()
  }

}
