package com.igorgoose.racehelpr.scraper.websocket

import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerFactory
import com.igorgoose.racehelpr.scraper.kartchrono.KartchronoMockServerWebSocketHandler
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
    fun handlerMapping(kafkaConsumerFactory: KafkaConsumerFactory, objectMapper: ObjectMapper): HandlerMapping =
        SimpleUrlHandlerMapping(
            mapOf(
                "/mockserver" to KartchronoMockServerWebSocketHandler(
                    kafkaConsumerFactory,
                    objectMapper
                )
            ), -1
        )
}
