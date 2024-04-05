package com.igorgoose.racehelpr.scraper.kafka

import com.igorgoose.racehelpr.scraper.label.LabelManager
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener

@Configuration
class KafkaConfig(
    @Value("\${kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${kafka.topics.names}") private val topics: List<String>,
    @Value("\${kafka.topics.max-size-bytes}") private val maxTopicSize: Long,
    @Value("\${kafka.topics.recreate}") private val recreate: Boolean,
    private val labelManager: LabelManager,
) {
    private val logger = KotlinLogging.logger { }

    @Bean
    fun kafkaProducer() = createProducer(bootstrapServers)

    @EventListener(ApplicationStartedEvent::class)
    fun createKafkaTopics(event: ApplicationStartedEvent) {
        logger.info { "Creating necessary kafka topics" }
        createTopicsIfNotExist(bootstrapServers, topics, logger, maxTopicSize, recreate).also {
            if (recreate) {
                labelManager.truncateLabels()
            }
        }
    }
}
