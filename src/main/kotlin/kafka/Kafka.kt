package kafka

import app.*
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import serializeToJson
import util.ConfigLoader
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

class Kafka {
    val notStopping = AtomicBoolean(false)
    var kafkaConsumer: KafkaConsumer<String, ByteArray>
    var latch = CountDownLatch(1)
    val configs = mutableMapOf<String, Any>()
    private val schemaRegistryClient: SchemaRegistryClient

    init {
        val config = ConfigLoader()
        val bootstrapServers = config.getValue("CONFLUENT_BOOTSTRAP_SERVERS")
        val confluentId = config.getValue("CONFLUENT_ID")
        val confluentSecret = config.getValue("CONFLUENT_SECRET")
        val schemaRegistryId = config.getValue("SCHEMA_REGISTRY_ID")
        val schemaRegistrySecret = config.getValue("SCHEMA_REGISTRY_SECRET")
        val schemaRegistryRest = config.getValue("SCHEMA_REGISTRY_REST")

        configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs["security.protocol"] = "SASL_SSL"
        configs["sasl.mechanism"] = "PLAIN"
        configs["sasl.jaas.config"] =
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$confluentId\" password=\"$confluentSecret\";"

        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            org.apache.kafka.common.serialization.StringDeserializer::class.java.name
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            org.apache.kafka.common.serialization.ByteArrayDeserializer::class.java.name
        configs[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryRest
        configs[AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE] = "USER_INFO"

        configs[AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG] = "$schemaRegistryId:$schemaRegistrySecret"

        schemaRegistryClient = CachedSchemaRegistryClient(
            configs[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] as String,
            1000, // schema cache size

            mapOf(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to configs[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG],
                AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE to "USER_INFO",
                AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG to configs[AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG]
            ),
            mapOf()
        )


        kafkaConsumer = KafkaConsumer(
            configs
        )
        val thread = Thread {
            while (true) {
                when (val msg = Channels.kafkaChannel.take()) {

                    is ListTopics -> {
                        msg.result.complete(list())
                    }

                    is ListenToTopic -> {
                        addLogsToIndex(msg.name)
                    }

                    is UnListenToTopics -> {
                        //   Channels.cmdChannel.put(ClearIndex)
                        if (notStopping.get()) {
                            latch = CountDownLatch(1)
                            notStopping.set(false)
                            latch.await()
                        }
                    }

                    is PublishToTopic -> {
                        val configs = mutableMapOf<String, Any>()
                        configs["bootstrap.servers"] = "localhost:9094"
                        val kafkaProducer = KafkaProducer(configs, StringSerializer(), StringSerializer())
                        kafkaProducer.send(ProducerRecord(msg.topic, msg.key, msg.value))
                    }
                }
            }
        }
        thread.isDaemon = true
        thread.start()
    }


    private fun list(): List<String> {
        val topic = kafkaConsumer.listTopics().map { it.key }.sorted()
        return topic
    }


    @OptIn(DelicateCoroutinesApi::class)
    private fun addLogsToIndex(topics: List<String>): AtomicBoolean {
        val threadDispatcher = newSingleThreadContext("CoroutineThread")
        val scope = CoroutineScope(threadDispatcher)
        //  Channels.cmdChannel.put(ClearIndex)
        notStopping.set(true)
        scope.launch {
            Channels.popChannel.send(ClearIndex)
            val map =
                kafkaConsumer.listTopics().filter { topics.contains(it.key) }.map { it.value }.flatten()
                    .map { TopicPartition(it.topic(), it.partition()) }

            kafkaConsumer.assign(map)
            val endOffsets = kafkaConsumer.endOffsets(map)


            val offsetsForTimes = kafkaConsumer.offsetsForTimes(map.associateWith {
                OffsetDateTime.now().minus(Duration.ofDays(State.kafkaDays.get())).toEpochSecond() * 1000
            })
            offsetsForTimes.forEach {
                kafkaConsumer.seek(it.key, it.value?.offset() ?: endOffsets[it.key]!!)
            }


            while (notStopping.get()) {
                try {
                    if (kafkaConsumer.assignment().size == 0) {
                        notStopping.set(false)
                        continue
                    }
                    val poll: ConsumerRecords<String, ByteArray> = kafkaConsumer.poll(Duration.ofMillis(500))
                    poll.forEach {

                        val rawBytes = it.value()
                        val value = when {
                            rawBytes != null -> try {
                                val avroDeserializer = KafkaAvroDeserializer(schemaRegistryClient)
                                val avroRecord = avroDeserializer.deserialize(it.topic(), rawBytes)
                                avroRecord.toString()
                            } catch (e: Exception) {
                                try {
                                    String(rawBytes)
                                } catch (e: Exception) {
                                    rawBytes.toString()
                                }
                            }

                            else -> "null"
                        }

                        val message = Message(
                            offset = it.offset().toInt(),
                            partition = it.partition(),
                            z = value.toString(), timestamp = OffsetDateTime.ofInstant(
                                Instant.ofEpochMilli(it.timestamp()),
                                ZoneId.systemDefault()
                            ), key = it.key() ?: "", topic = it.topic()
                        ).serializeToJson()
                        Channels.popChannel.send(
                            AddToIndex(
                                "${Instant.ofEpochMilli(it.timestamp())} ${message}",
                                it.topic()
                            )
                        )
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
            latch.countDown()
        }
        return notStopping
    }
}

class Message(
    val topic: String,
    val key: String,
    val timestamp: OffsetDateTime,
    val partition: Int,
    val offset: Int,
    @JsonDeserialize(using = RawJsonDeserializer::class) @JsonSerialize(using = RawJsonSerializer::class) val z: String,
)

class RawJsonDeserializer : JsonDeserializer<String>() {
    @Throws(IOException::class)
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): String {
        val mapper = jp.codec as ObjectMapper
        val node = mapper.readTree<JsonNode>(jp)
        return mapper.writeValueAsString(node)
    }
}

class RawJsonSerializer : JsonSerializer<String?>() {
    @Throws(IOException::class)
    override fun serialize(value: String?, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeRawValue(value)
    }
}
