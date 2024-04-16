package com.igorgoose.racehelpr.scraper.websocket

import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerUtils
import com.igorgoose.racehelpr.scraper.kartchrono.KartchronoSessionManager
import com.igorgoose.racehelpr.scraper.label.LabelManager
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.client.StandardWebSocketClient

@Configuration
class WebSocketConfig {
    @Bean
    fun webSocketClient() = StandardWebSocketClient()

    @Bean
    fun handlerMapping(
        kafkaConsumerUtils: KafkaConsumerUtils,
        labelManager: LabelManager,
        objectMapper: ObjectMapper,
        kartchronoSessionManager: KartchronoSessionManager
    ): HandlerMapping = SimpleUrlHandlerMapping(mapOf("/mockserver" to kartchronoSessionManager), -1)
}
