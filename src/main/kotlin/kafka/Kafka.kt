package kafka

import LogLevel
import State
import app.*
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.coroutines.*
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

    // Pool of deserializers for parallel processing
    private val deserializerPool = mutableListOf<KafkaAvroDeserializer>()
    private val deserializerLock = ReentrantLock()

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

        // Optimize fetch settings for larger batches
        configs[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1000
        configs[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 1024 * 1024 // 1MB
        configs[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 500
        configs[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = 1 * 1024 * 1024 // 1MB

        // Enable compression for network efficiency
        configs["compression.type"] = "snappy" // Fast compression/decompression

        configs[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryRest

        schemaRegistryClient = if (schemaRegistryId.isBlank()) {
            CachedSchemaRegistryClient(
                schemaRegistryRest, 1000,
                mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryRest),
                mapOf()
            )
        } else {
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

        // Initialize deserializer pool
        repeat(Runtime.getRuntime().availableProcessors()) {
            deserializerPool.add(KafkaAvroDeserializer(schemaRegistryClient))
        }

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
        val groups = adminClient.listGroups().all().get()

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

    private fun getDeserializer(): KafkaAvroDeserializer {
        deserializerLock.withLock {
            return if (deserializerPool.isNotEmpty()) {
                deserializerPool.removeAt(0)
            } else {
                KafkaAvroDeserializer(schemaRegistryClient)
            }
        }
    }

    private fun returnDeserializer(deserializer: KafkaAvroDeserializer) {
        deserializerLock.withLock {
            if (deserializerPool.size < Runtime.getRuntime().availableProcessors() * 2) {
                deserializerPool.add(deserializer)
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
    private fun startConsuming(topics: List<String>) {
        val dispatcher = newSingleThreadContext("KafkaConsumerThread")
        val scope = CoroutineScope(dispatcher + SupervisorJob())
        notStopping.set(true)

        // Dispatcher for parallel processing
        val processingDispatcher = Dispatchers.Default

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
                        val currentAssignment = kafkaConsumer.assignment()
                        if (currentAssignment.isEmpty()) {
                            ConsumerRecords.empty()
                        } else {
                            kafkaConsumer.poll(Duration.ofMillis(500))
                        }
                    }

                    if (records.isEmpty) {
                        continue
                    }

                    // Group records by partition - each partition processed on one thread
                    val recordsByPartition = records.groupBy {
                        TopicPartition(it.topic(), it.partition())
                    }

                    // Process each partition in parallel (1 partition = 1 thread maintains order)
                    val jobs = recordsByPartition.map { (_, partitionRecords) ->
                        async(processingDispatcher) {
                            for (record in partitionRecords) {
                                try {
                                    val deserializer = getDeserializer()
                                    try {
                                        val value = try {
                                            deserializer.deserialize(record.topic(), record.value()).toString()
                                        } catch (e: Exception) {
                                            String(record.value() ?: ByteArray(0))
                                        }

                                        val headers = record.headers().toList()
                                        val headersMap = headers.associate { it.key() to it.value() }

                                        val kafkaLine = KafkaLineDomain(
                                            seq = globalSeq.getAndAdd(1),
                                            level = LogLevel.KAFKA,
                                            timestamp = record.timestamp(),
                                            key = record.key(),
                                            message = value,
                                            indexIdentifier = "${record.topic()}#${record.partition()}",
                                            offset = record.offset(),
                                            partition = record.partition(),
                                            topic = record.topic(),
                                            headers = headers.joinToString(" | ") {
                                                "${it.key()} : ${it.value()?.toString(Charsets.UTF_8)}"
                                            },
                                            correlationId = headersMap["X-Correlation-Id"]?.let { String(it) },
                                            requestId = headersMap["X-Request-Id"]?.let { String(it) },
                                            compositeEventId = "${record.topic()}#${record.partition()}#${record.offset()}"
                                        )

                                        Channels.popChannel.send(AddToIndexDomainLine(kafkaLine))
                                    } finally {
                                        returnDeserializer(deserializer)
                                    }
                                } catch (e: Exception) {
                                    e.printStackTrace()
                                }
                            }
                        }
                    }

                    // Wait for all partitions to complete
                    jobs.awaitAll()

                } catch (e: Exception) {
                    e.printStackTrace()
                    try {
                        Thread.sleep(1000)
                    } catch (interrupted: InterruptedException) {
                        Thread.currentThread().interrupt()
                        break
                    }
                }
            }

            println("Kafka consumer stopped for topics: $topics")
            latch.countDown()
        }
    }

    fun fetchMessage(topic: String, partition: Int, offset: Long): KafkaLineDomain? {
        val tempConsumer = KafkaConsumer<String, ByteArray>(configs)

        try {
            val topicPartition = TopicPartition(topic, partition)
            tempConsumer.assign(listOf(topicPartition))
            tempConsumer.seek(topicPartition, offset)

            val records = tempConsumer.poll(Duration.ofMillis(1000))
            val record = records.records(topicPartition).firstOrNull()

            if (record != null) {
                val deserializer = getDeserializer()
                try {
                    val value = try {
                        deserializer.deserialize(record.topic(), record.value()).toString()
                    } catch (e: Exception) {
                        String(record.value() ?: ByteArray(0))
                    }

                    val headers = record.headers().toList()
                    val headersMap = headers.associate { it.key() to it.value() }

                    return KafkaLineDomain(
                        seq = globalSeq.getAndAdd(1),
                        level = LogLevel.KAFKA,
                        timestamp = record.timestamp(),
                        key = record.key(),
                        message = value,
                        indexIdentifier = "${record.topic()}#${record.partition()}",
                        offset = record.offset(),
                        partition = record.partition(),
                        topic = record.topic(),
                        headers = headers.joinToString(" | ") {
                            "${it.key()} : ${it.value()?.toString(Charsets.UTF_8)}"
                        },
                        correlationId = headersMap["X-Correlation-Id"]?.let { String(it) },
                        requestId = headersMap["X-Request-Id"]?.let { String(it) },
                        compositeEventId = "${record.topic()}#${record.partition()}#${record.offset()}"
                    )
                } finally {
                    returnDeserializer(deserializer)
                }
            }
            return null
        } catch (e: Exception) {
            e.printStackTrace()
            return null
        } finally {
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