package com.igorgoose.racehelpr.scraper.kafka

import io.github.oshai.kotlinlogging.KLogger
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import java.util.concurrent.ExecutionException


fun createTopicsIfNotExist(
    bootstrapServers: String,
    topics: List<String>,
    logger: KLogger,
    maxTopicSize: Long,
    recreate: Boolean
) {
    val props = Properties().also {
        it[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        it[AdminClientConfig.CLIENT_ID_CONFIG] = "scraper-admin"
        it[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = 5000
        it[AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG] = 5000
    }
    val admin = Admin.create(props)

    if (recreate) {
        try {
            logger.debug { "deleting topics $topics" }
            admin.deleteTopics(topics).all().get().also {
                logger.debug { "topics have been deleted" }
            }
        } catch (ex: ExecutionException) {
            if (ex.cause is UnknownTopicOrPartitionException) logger.debug { "topics have been deleted" }
        }
    }

    try {
        logger.debug { "creating topics: $topics" }
        admin.createTopics(topics.map {
            NewTopic(it, 1, -1).configs(
                mapOf(
                    "compression.type" to "gzip",
                    "retention.bytes" to maxTopicSize.toString(),
                    "retention.ms" to "-1"
                )
            )
        })
            .all().get()
            .also {
                logger.debug { "topics have been created" }
            }
    } catch (ex: ExecutionException) {
        if (ex.cause is TopicExistsException) logger.debug {
            ex.cause!!.message
        }
    }
}

fun createProducer(bootstrapServers: String): KafkaProducer<Int?, String> {
    val props = Properties().also {
        // bootstrap server config is required for producer to connect to brokers
        it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        // client id is not required, but it's good to track the source of requests beyond just ip/port
        // by allowing a logical application name to be included in server-side request logging
        it[ProducerConfig.CLIENT_ID_CONFIG] = "scraper-producer"
        // key and value are just byte arrays, so we need to set appropriate serializers
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        it[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] = 5000
        it[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 5000
    }
    return KafkaProducer<Int?, String>(props)
}
