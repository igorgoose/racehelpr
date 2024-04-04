package com.igorgoose.racehelpr.scraper.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.ContextStartedEvent
import org.springframework.context.event.EventListener

@Configuration
class KafkaConfig(
    @Value("\${kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${kafka.topics}") private val topics: List<String>
) {
    private val logger = KotlinLogging.logger {  }

    @Bean
    fun kafkaProducer() = createProducer(bootstrapServers)

    @EventListener
    fun createKafkaTopics(event: ContextStartedEvent) = createTopicsIfNotExist(bootstrapServers, topics, logger, true)
}