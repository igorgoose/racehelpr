package com.igorgoose.racehelpr.scraper.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.*

@Component("scraperKafkaConsumerFactory")
class KafkaConsumerFactory(
    @Value("\${kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${scraper.topic}") private val topic: String
) {
    fun createScraperConsumer(offset: Long): KafkaConsumer<Int?, String> {
        val props = Properties().apply {
            this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            this[ConsumerConfig.CLIENT_ID_CONFIG] = "scraper-consumer"
//            this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = true
            this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = IntegerDeserializer::class.java
            this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        }
        return KafkaConsumer<Int?, String>(props).also {
            it.assign(listOf(TopicPartition(topic, 0)))
            it.seek(TopicPartition(topic, 0), offset)
        }
    }
}