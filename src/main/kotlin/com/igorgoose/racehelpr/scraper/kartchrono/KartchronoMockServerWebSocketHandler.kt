package com.igorgoose.racehelpr.scraper.kartchrono

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerFactory
import com.igorgoose.racehelpr.scraper.kartchrono.model.KartchronoDataRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.withContext
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Component
class KartchronoMockServerWebSocketHandler(
    private val kafkaConsumerFactory: KafkaConsumerFactory,
    private val jacksonObjectMapper: ObjectMapper
) : WebSocketHandler {
    private val logger = KotlinLogging.logger { }

    override fun handle(session: WebSocketSession): Mono<Void> {
        return session.send(createMessageFlow(session).asFlux())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun createMessageFlow(session: WebSocketSession): Flow<WebSocketMessage> {
        logger.debug { "Receiving kartchrono messages" }
        return session.receive()
            .doOnNext { it.retain() }
            .asFlow()
            .onEach { logger.debug { "Received $it" } }
            .filter { it.checkIfTextReleaseOtherwise() }
            .map { it.getTextAndRelease() }
            .filter { isProcessableMessage(it) }
            .flatMapLatest { message ->
                val request = jacksonObjectMapper.readValue(message, KartchronoDataRequest::class.java)
                logger.debug { "Received start message -> starting to send data" }

                flow {
                    try {
                        kafkaConsumerFactory.createScraperConsumer(offset = request.offset).use { consumer ->
                            while (true) {
                                logger.debug { "Consuming kartchrono messages from kafka" }
                                val records = withContext(Dispatchers.IO) {
                                    consumer.poll(1.seconds.toJavaDuration())
                                }

                                records.forEach { r ->
                                    logger.debug { "Got kafka record[offset=${r.offset()}, value=${r.value()}]" }
                                    emit(session.textMessage(r.value()))
                                }
                            }
                        }
                    } catch (ex: Throwable) {
                        logger.error(ex) { "Error occurred during reading kafka messages" }
                    } finally {
                        logger.info { "Completed producing kartchrono messages" }
                    }
                }
            }
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
}

