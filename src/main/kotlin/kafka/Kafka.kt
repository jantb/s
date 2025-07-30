package kafka

import LogLevel
import State
import State.offset
import app.*
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.coroutines.*
import kotlinx.datetime.Instant
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import util.ConfigLoader
import java.io.IOException
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class Kafka {
    val notStopping = AtomicBoolean(false)
    private val kafkaConsumer: KafkaConsumer<String, ByteArray>
    private val adminClient: AdminClient
    private val configs = mutableMapOf<String, Any>()
    private val schemaRegistryClient: SchemaRegistryClient
    private var latch = CountDownLatch(1)
    private val lock = ReentrantLock()

    init {
        val config = ConfigLoader()
        val bootstrapServers = config.getValue("CONFLUENT_BOOTSTRAP_SERVERS")
        val confluentId = config.getValue("CONFLUENT_ID")
        val confluentSecret = config.getValue("CONFLUENT_SECRET")
        val schemaRegistryId = config.getValue("SCHEMA_REGISTRY_ID")
        val schemaRegistrySecret = config.getValue("SCHEMA_REGISTRY_SECRET")
        val schemaRegistryRest = config.getValue("SCHEMA_REGISTRY_REST")

        configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        if (confluentId.isNotBlank()) {
            configs["security.protocol"] = "SASL_SSL"
            configs["sasl.mechanism"] = "PLAIN"
            configs["sasl.jaas.config"] =
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$confluentId\" password=\"$confluentSecret\";"
        }

        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java.name

        configs[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryRest

        schemaRegistryClient = if (schemaRegistryId.isBlank()) {
            CachedSchemaRegistryClient(
                schemaRegistryRest, 1000,
                mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryRest),
                mapOf()
            )
        } else {
            configs[AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE] = "USER_INFO"
            configs[AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG] = "$schemaRegistryId:$schemaRegistrySecret"

            CachedSchemaRegistryClient(
                schemaRegistryRest, 1000,
                mapOf(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryRest,
                    AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE to "USER_INFO",
                    AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG to "$schemaRegistryId:$schemaRegistrySecret"
                ),
                mapOf()
            )
        }

        kafkaConsumer = KafkaConsumer(configs)
        adminClient = AdminClient.create(configs)

        Thread {
            while (true) {
                when (val msg = Channels.kafkaChannel.take()) {
                    is ListTopics -> {
                        msg.result.complete(listTopics())
                    }

                    is ListLag -> {
                        msg.result.complete(listLag())
                    }

                    is ListenToTopic -> {
                        startConsuming(msg.name)
                    }

                    is UnListenToTopics -> {
                        if (notStopping.get()) {
                            latch = CountDownLatch(1)
                            notStopping.set(false)
                            latch.await()
                        }
                    }
                    
                    is UnassignTopics -> {
                        lock.withLock {
                            val currentAssignment = kafkaConsumer.assignment()
                            val partitionsToUnassign = currentAssignment.filter { topicPartition ->
                                topicPartition.topic() in msg.topics
                            }
                            val newAssignment = currentAssignment - partitionsToUnassign
                            kafkaConsumer.assign(newAssignment)
                        }
                    }

                    is PublishToTopic -> {
                        val producerConfigs = mapOf("bootstrap.servers" to "localhost:19092")
                        val kafkaProducer = KafkaProducer(producerConfigs, StringSerializer(), StringSerializer())
                        kafkaProducer.send(ProducerRecord(msg.topic, msg.key, msg.value))
                    }
                }
            }
        }.apply { isDaemon = true }.start()
    }

    private fun listTopics(): List<String> = lock.withLock {
        kafkaConsumer.listTopics().map { it.key }.sorted()
    }

    private fun listLag(): List<LagInfo> = lock.withLock {
        val lagInfoList = mutableListOf<LagInfo>()
        val groups = adminClient.listConsumerGroups().all().get()

        groups.forEach { group ->
            val groupId = group.groupId()
            val consumerOffsets = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()

            val endOffsets = kafkaConsumer.endOffsets(consumerOffsets.keys)

            consumerOffsets.forEach { (tp, offsetMeta) ->
                val end = endOffsets.getValue(tp)
                val current = offsetMeta.offset()
                lagInfoList.add(
                    LagInfo(
                        groupId = groupId,
                        topic = tp.topic(),
                        partition = tp.partition(),
                        currentOffset = current,
                        endOffset = end,
                        lag = end - current
                    )
                )
            }
        }

        lagInfoList
    }

    @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
    private fun startConsuming(topics: List<String>) {
        val dispatcher = newSingleThreadContext("KafkaConsumerThread")
        val scope = CoroutineScope(dispatcher)
        notStopping.set(true)

        scope.launch {
            lock.withLock {
                val partitions = kafkaConsumer.listTopics()
                    .filter { it.key in topics }
                    .flatMap { it.value.map { tp -> TopicPartition(tp.topic(), tp.partition()) } }

                kafkaConsumer.assign(partitions)

                val startOffsets = kafkaConsumer.offsetsForTimes(partitions.associateWith {
                    OffsetDateTime.now().minusDays(State.kafkaDays.get()).toEpochSecond() * 1000
                })

                partitions.forEach {
                    val startOffset = startOffsets[it]?.offset()
                        ?: kafkaConsumer.endOffsets(listOf(it))[it]
                        ?: 0L
                    kafkaConsumer.seek(it, startOffset)
                }
            }

            while (notStopping.get()) {
                try {
                    val records: ConsumerRecords<String, ByteArray> = lock.withLock {
                        if (kafkaConsumer.assignment().isEmpty()) {
                            notStopping.set(false)
                            return@withLock ConsumerRecords.empty()
                        }
                        kafkaConsumer.poll(Duration.ofMillis(500))
                    }

                    for (record in records) {
                        val value = try {
                            KafkaAvroDeserializer(schemaRegistryClient).deserialize(record.topic(), record.value())
                                .toString()
                        } catch (e: Exception) {
                            String(record.value() ?: ByteArray(0))
                        }

                        val headers = record.headers().toList()
                        val kafkaLine = KafkaLineDomain(
                            seq = seq.getAndAdd(1),
                            level = LogLevel.KAFKA,
                            timestamp = record.timestamp(),
                            key = record.key(),
                            message = value,
                            indexIdentifier = "${record.topic()}#${record.partition()}",
                            offset = record.offset(),
                            partition = record.partition(),
                            topic = record.topic(),
                            headers = headers.joinToString(" | ") { "${it.key()} : ${it.value()?.toString(Charsets.UTF_8)}" },
                            correlationId = headers.associate { it.key() to it.value() }["X-Correlation-Id"]?.let { String(it) },
                            requestId = headers.associate { it.key() to it.value() }["X-Request-Id"]?.let { String(it) },
                            compositeEventId = "${record.topic()}#${record.partition()}#${record.offset()}"
                        )

                        Channels.popChannel.send(AddToIndexDomainLine(kafkaLine))
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }

            latch.countDown()
        }
    }

    fun fetchMessage(topic: String, partition: Int, offset: Long): KafkaLineDomain? {
        // Create a temporary consumer for fetching the specific message
        val tempConsumer = KafkaConsumer<String, ByteArray>(configs)
        val tempSchemaRegistryClient = schemaRegistryClient
        
        try {
            val topicPartition = TopicPartition(topic, partition)
            tempConsumer.assign(listOf(topicPartition))
            tempConsumer.seek(topicPartition, offset)
            
            val records = tempConsumer.poll(Duration.ofMillis(1000))
            val record = records.records(topicPartition).firstOrNull()
            
            if (record != null) {
                val value = try {
                    KafkaAvroDeserializer(tempSchemaRegistryClient).deserialize(record.topic(), record.value())
                        .toString()
                } catch (e: Exception) {
                    String(record.value() ?: ByteArray(0))
                }
                
                val headers = record.headers().toList()
                return KafkaLineDomain(
                    seq = seq.getAndAdd(1),
                    level = LogLevel.KAFKA,
                    timestamp = record.timestamp(),
                    key = record.key(),
                    message = value,
                    indexIdentifier = "${record.topic()}#${record.partition()}",
                    offset = record.offset(),
                    partition = record.partition(),
                    topic = record.topic(),
                    headers = headers.joinToString(" | ") { "${it.key()} : ${it.value()?.toString(Charsets.UTF_8)}" },
                    correlationId = headers.associate { it.key() to it.value() }["X-Correlation-Id"]?.let { String(it) },
                    requestId = headers.associate { it.key() to it.value() }["X-Request-Id"]?.let { String(it) },
                    compositeEventId = "${record.topic()}#${record.partition()}#${record.offset()}"
                )
            }
            return null
        } catch (e: Exception) {
            e.printStackTrace()
            return null
        } finally {
            // Close the temporary consumer
            tempConsumer.close()
        }
    }

    data class LagInfo(
        val groupId: String,
        val topic: String,
        val partition: Int,
        val currentOffset: Long,
        val endOffset: Long,
        val lag: Long
    )
}

class RawJsonDeserializer : JsonDeserializer<String>() {
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): String {
        val mapper = jp.codec as ObjectMapper
        val node = mapper.readTree<JsonNode>(jp)
        return mapper.writeValueAsString(node)
    }
}

class RawJsonSerializer : JsonSerializer<String?>() {
    override fun serialize(value: String?, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeRawValue(value)
    }
}
