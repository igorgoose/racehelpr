package com.igorgoose.racehelpr.scraper.websocket

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.socket.client.StandardWebSocketClient

@Configuration
class WebSocketConfig {
    @Bean
    fun webSocketClient() = StandardWebSocketClient()
}