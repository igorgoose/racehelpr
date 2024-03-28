package com.igorgoose.racehelpr

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

const val BOOTSTRAP_SERVERS = "localhost:9092"

fun createTopicsIfNotExist(topics: List<String>, recreate: Boolean = false) {
    val props = Properties().also {
        it[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        it[AdminClientConfig.CLIENT_ID_CONFIG] = "scraper-admin"
        it[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = 5000
        it[AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG] = 5000
    }
    val admin = Admin.create(props)

    if (recreate) {
        try {
            println("deleting topics $topics")
            admin.deleteTopics(topics).all().get().also {
                println("topics have been deleted")
            }
        } catch (ex: ExecutionException) {
            if (ex.cause is UnknownTopicOrPartitionException) println("topics have been deleted")
        }
    }

    try {
        println("creating topics: $topics")
        admin.createTopics(topics.map { NewTopic(it, 1, -1) }).all().get().also {
            println("topics have been created")
        }
    } catch (ex: ExecutionException) {
        if (ex.cause is TopicExistsException) println(ex.cause!!.message)
    }
}

fun createProducer(): KafkaProducer<Int, String> {
    val props = Properties().also {
        // bootstrap server config is required for producer to connect to brokers
        it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        // client id is not required, but it's good to track the source of requests beyond just ip/port
        // by allowing a logical application name to be included in server-side request logging
        it[ProducerConfig.CLIENT_ID_CONFIG] = "scraper-producer"
        // key and value are just byte arrays, so we need to set appropriate serializers
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        it[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] = 5000
        it[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 5000
    }
    return KafkaProducer<Int, String>(props)
}
