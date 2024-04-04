package com.igorgoose.racehelpr.scraper.websocket

import com.igorgoose.racehelpr.scraper.label.LabelManager
import com.igorgoose.racehelpr.scraper.schedule.ScraperTask
import org.apache.kafka.clients.producer.KafkaProducer
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class WebSocketHandlerFactory(
    private val producer: KafkaProducer<Int, String>,
    private val labelManager: LabelManager,
    @Value("\${scraper.topic}") private val scraperTopic: String
) {
    fun createScraper(task: ScraperTask) = ScraperJob(producer, scraperTopic, labelManager, task)
}