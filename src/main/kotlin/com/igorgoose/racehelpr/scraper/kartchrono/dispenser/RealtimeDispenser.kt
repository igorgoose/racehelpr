package com.igorgoose.racehelpr.scraper.kartchrono.dispenser

import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerUtils
import kotlinx.coroutines.flow.FlowCollector
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession

class RealtimeDispenser(
    private val session: WebSocketSession,
    private val messageCollector: FlowCollector<WebSocketMessage>,
    private val kafkaConsumerUtils: KafkaConsumerUtils,
    private val consumer: KafkaConsumer<Int?, String>,
    private val offset: Long,
): Dispenser {
    override suspend fun dispense() {

    }

    override fun react(message: WebSocketMessage) {

    }
}