package com.igorgoose.racehelpr.scraper.kartchrono

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerUtils
import com.igorgoose.racehelpr.scraper.kartchrono.model.ApplyConfigurationRequest
import com.igorgoose.racehelpr.scraper.kartchrono.model.KartchronoTrackRequest
import com.igorgoose.racehelpr.scraper.label.LabelManager
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.EmptyCoroutineContext

@Component
class KartchronoSessionManager(
    private val kafkaConsumerUtils: KafkaConsumerUtils,
    private val jacksonObjectMapper: ObjectMapper,
    private val labelManager: LabelManager
) : WebSocketHandler {
    private val logger = KotlinLogging.logger { }
    private val sessionsById = ConcurrentHashMap<String, KartchronoSession>()

    override fun handle(session: WebSocketSession): Mono<Void> {
        KartchronoSession(
            session = session,
            labelManager = labelManager,
            kafkaConsumerUtils = kafkaConsumerUtils,
            context = EmptyCoroutineContext,
            initConsumer = { kafkaConsumerUtils.createScraperConsumer() }
        ).also {
            sessionsById[session.id] = it
        }

        return session.send(createMessageFlow(session).asFlux())
            .doFinally {
                logger.debug { "Removing session ${session.id}" }
                sessionsById[session.id]?.close()
                sessionsById.remove(session.id)
            }
    }

    suspend fun applyConfiguration(sessionId: String, configurationRequest: ApplyConfigurationRequest) {
        sessionsById[sessionId]?.applyConfiguration(configurationRequest)
            ?: error("No session found with id $sessionId")
    }

    suspend fun pauseSession(sessionId: String) {
        sessionsById[sessionId]?.pause() ?: error("No session found with id $sessionId")
    }

    suspend fun produce(sessionId: String, count: Int) {
        sessionsById[sessionId]?.produce(count) ?: error("No session found with id $sessionId")
    }

    suspend fun produceToOffset(sessionId: String, offset: Long) {
        sessionsById[sessionId]?.produceTo(offset) ?: error("No session found with id $sessionId")
    }

    suspend fun produceToLabel(sessionId: String, label: String) {
        sessionsById[sessionId]?.produceTo(label) ?: error("No session found with id $sessionId")
    }

    suspend fun fastForward(sessionId: String, count: Int) {
        sessionsById[sessionId]?.fastForward(count) ?: error("No session found with id $sessionId")
    }

    suspend fun fastForwardToOffset(sessionId: String, offset: Long) {
        sessionsById[sessionId]?.fastForwardTo(offset) ?: error("No session found with id $sessionId")
    }

    suspend fun fastForwardToLabel(sessionId: String, label: String) {
        sessionsById[sessionId]?.fastForwardTo(label) ?: error("No session found with id $sessionId")
    }

    fun setLabel(sessionId: String, label: String) {
        sessionsById[sessionId]?.setLabel(label) ?: error("No session found with id $sessionId")
    }

    @OptIn(ExperimentalCoroutinesApi::class) // flatMapLatest
    private fun createMessageFlow(session: WebSocketSession): Flow<WebSocketMessage> {
        logger.debug { "Receiving kartchrono messages" }
        return session.receive()
            .doOnNext { it.retain() }
            .asFlow()
            .onEach { logger.debug { "Received $it" } }
            .filter { it.checkIfTextOtherwiseRelease() }
            .map { parseRequest(it.getTextAndRelease()) }
            .filterNotNull()
            .flatMapLatest { dispenseForTrack(it.trackId, session) }
    }

    private suspend fun dispenseForTrack(trackId: String, session: WebSocketSession): Flow<WebSocketMessage> {
        val kartchronoSession = sessionsById[session.id] ?: error("No session found with id ${session.id}")
        return flow {
            kartchronoSession.start(this)
        }
    }

    private fun parseRequest(message: String): KartchronoTrackRequest? {
        try {
            return jacksonObjectMapper.readValue(message, KartchronoTrackRequest::class.java)
        } catch (e: JsonMappingException) {
            logger.debug(e) { "Got unprocessable message" }
            return null
        }
    }

    private fun WebSocketMessage.checkIfTextOtherwiseRelease(): Boolean = when (type) {
        WebSocketMessage.Type.TEXT -> true
        else -> {
            release()
            false
        }
    }

    private fun WebSocketMessage.getTextAndRelease(): String {
        return payloadAsText.also {
            release()
        }
    }

}
