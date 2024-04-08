package com.igorgoose.racehelpr.scraper.kartchrono

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerFactory
import com.igorgoose.racehelpr.scraper.kartchrono.model.KartchronoDataRequest
import com.igorgoose.racehelpr.scraper.label.LabelManager
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Component
class KartchronoMockServerWebSocketHandler(
    private val kafkaConsumerFactory: KafkaConsumerFactory,
    private val jacksonObjectMapper: ObjectMapper,
    private val labelManager: LabelManager
) : WebSocketHandler {
    private val logger = KotlinLogging.logger { }

    override fun handle(session: WebSocketSession): Mono<Void> {
        return session.send(createMessageFlow(session).asFlux())
    }

    @OptIn(ExperimentalCoroutinesApi::class) // flatMapLatest
    private fun createMessageFlow(session: WebSocketSession): Flow<WebSocketMessage> {
        logger.debug { "Receiving kartchrono messages" }
        return session.receive()
            .doOnNext { it.retain() }
            .asFlow()
            .onEach { logger.debug { "Received $it" } }
            .filter { it.checkIfTextReleaseOtherwise() }
            .map { it.getTextAndRelease() }
            .filter { isProcessableMessage(it) }
            .flatMapLatest { message -> createRequestedDataFlow(message, session) }
    }

    private fun isProcessableMessage(message: String): Boolean {
        var ex: Exception? = null
        try {
            jacksonObjectMapper.readValue(message, KartchronoDataRequest::class.java)
        } catch (e: JsonMappingException) {
            ex = e
        }
        return ex == null
    }

    private fun WebSocketMessage.checkIfTextReleaseOtherwise(): Boolean = when (type) {
        WebSocketMessage.Type.TEXT -> true
        else -> {
            release()
            false
        }
    }

    private fun WebSocketMessage.getTextAndRelease(): String {
        val text = payloadAsText
        release()
        return text
    }

    private fun createRequestedDataFlow(message: String, session: WebSocketSession): Flow<WebSocketMessage> {
        val request = jacksonObjectMapper.readValue(message, KartchronoDataRequest::class.java)
        logger.debug { "Received start message -> starting to send data" }

        return flow {
            try {
                kafkaConsumerFactory.createScraperConsumer(offset = request.getOffset()).use { consumer ->
                    while (true) {
                        logger.debug { "Consuming kartchrono messages from kafka" }
                        val records: ConsumerRecords<Int?, String> = withContext(Dispatchers.IO) {
                            consumer.poll(1.seconds.toJavaDuration())
                        }
                        emitRecords(records, session, request.realTimeMode)
                    }
                }
            } catch (ex: Throwable) {
                logger.error(ex) { "Error occurred during reading kafka messages" }
            } finally {
                logger.info { "Completed producing kartchrono messages" }
            }
        }
    }

    private fun KartchronoDataRequest.getOffset(): Long = label?.let {
        labelManager.getByValue(it)?.offset ?: error("No label '$it'")
    } ?: offset ?: error("Either offset or label must be present in request[request=$this]")

    // TODO consider delays between messages in batches
    private suspend fun FlowCollector<WebSocketMessage>.emitRecords(
        records: ConsumerRecords<Int?, String>,
        session: WebSocketSession,
        realTime: Boolean
    ) {
        if (realTime) {
            logger.debug { "Emitting kartchrono messages in realtime mode" }
            var prevTimestamp = -1L
            records.forEach { consumerRecord ->
                logger.debug { "Got kafka record[offset=${consumerRecord.offset()}, value=$consumerRecord.value()}]" }
                if (prevTimestamp != -1L) {
                    val delayMs = consumerRecord.timestamp() - prevTimestamp
                    logger.debug { "Delaying next record emission by ${delayMs.milliseconds}" }
                    delay(delayMs) // nitpicking: should also consider the time we spend emitting
                }
                prevTimestamp = consumerRecord.timestamp()
                emit(session.textMessage(consumerRecord.value()))
            }
        } else {
            logger.debug { "Emitting kartchrono messages instantly" }
            records.forEach { consumerRecord ->
                logger.debug { "Got kafka record[offset=${consumerRecord.offset()}, value=$consumerRecord.value()}]" }
                emit(session.textMessage(consumerRecord.value()))
            }
        }
    }
}

