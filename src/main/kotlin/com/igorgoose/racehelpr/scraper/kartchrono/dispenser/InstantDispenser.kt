package com.igorgoose.racehelpr.scraper.kartchrono.dispenser

import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class InstantDispenser(
    private val session: WebSocketSession,
    private val messageCollector: FlowCollector<WebSocketMessage>,
    private val kafkaConsumerUtils: KafkaConsumerUtils,
    private val consumer: KafkaConsumer<Int?, String>,
    private val initialOffset: Long,
) : Dispenser {
    private val logger = KotlinLogging.logger {}

    override suspend fun dispense() {
        kafkaConsumerUtils.setOffset(consumer, initialOffset)
        while (true) {
            logger.debug { "Consuming kartchrono messages from kafka" }
            val records: ConsumerRecords<Int?, String> = withContext(Dispatchers.IO) {
                consumer.poll(1.seconds.toJavaDuration())
            }
            for (record in records) {
                messageCollector.emit(session.textMessage(record.value()))
            }
        }
    }

    override fun react(message: WebSocketMessage) {
        // do nothing for now
    }

}