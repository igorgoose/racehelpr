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

    suspend fun fastForwardToOffset(sessionId: String, offset: Long) {
        sessionsById[sessionId]?.fastForwardTo(offset) ?: error("No session found with id $sessionId")
    }

    suspend fun fastForwardToLabel(sessionId: String, label: String) {
        sessionsById[sessionId]?.fastForwardTo(label) ?: error("No session found with id $sessionId")
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

//    private fun processMessage(message: WebSocketMessage, session: WebSocketSession): Flow<WebSocketMessage> {
//        val trackChosen = AtomicBoolean(false)
//        return flow {
//            suspendCancellableCoroutine { continuation ->
//
//            }
//            when (val stage = session.attributes["stage"]) {
//                SessionStage.INIT -> {
//                    val request = parseRequest(message.getTextAndRelease())
//                    if (request != null) {
//                        val dispenser = createDispenser(request.getOffset(), session)
//                        session.attributes["stage"] = SessionStage.DISPENSE
//                        session.attributes["dispenser"] = dispenser
//                        dispenser.dispense()
//                    }
//                }
//
//                SessionStage.DISPENSE -> {
//                    val dispenser = session.attributes["dispenser"] as? Dispenser
//                        ?: error("Illegal dispenser type ${session.attributes["dispenser"]}")
//                    dispenser.react(message)
//                }
//
//                else -> error("Unknown stage $stage")
//            }
//        }
//    }

//    private fun FlowCollector<WebSocketMessage>.createDispenser(offset: Long, session: WebSocketSession): Dispenser {
//        val consumer = kafkaConsumerUtils.createScraperConsumer()
//        return InstantDispenser(
//            session = session,
//            messageCollector = this,
//            kafkaConsumerUtils = kafkaConsumerUtils,
//            consumer = consumer,
//            initialOffset = offset
//        )
//    }


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

//    private fun createRequestedDataFlow(
//        request: ApplyConfigurationRequest,
//        session: WebSocketSession
//    ): Flow<WebSocketMessage> {
//        logger.debug { "Received start message -> starting to send data" }
//        return flow {
//            try {
//                kafkaConsumerUtils.createScraperConsumer(offset = request.getOffset()).use { consumer ->
//                    while (true) {
//                        logger.debug { "Consuming kartchrono messages from kafka" }
//                        val records: ConsumerRecords<Int?, String> = withContext(Dispatchers.IO) {
//                            consumer.poll(1.seconds.toJavaDuration())
//                        }
//                        emitRecords(records, session, true) // TODO replace with dispenser mode
//                    }
//                }
//            } catch (ex: Throwable) {
//                logger.error(ex) { "Error occurred during reading kafka messages" }
//            } finally {
//                logger.info { "Completed producing kartchrono messages" }
//            }
//        }
//    }
//
//
//    // TODO consider delays between messages in batches
//    private suspend fun FlowCollector<WebSocketMessage>.emitRecords(
//        records: ConsumerRecords<Int?, String>,
//        session: WebSocketSession,
//        realTime: Boolean
//    ) {
//        if (realTime) {
//            logger.debug { "Emitting kartchrono messages in realtime mode" }
//            var prevTimestamp = -1L
//            records.forEach { consumerRecord ->
//                logger.debug { "Got kafka record[offset=${consumerRecord.offset()}, value=$consumerRecord.value()}]" }
//                if (prevTimestamp != -1L) {
//                    val delayMs = consumerRecord.timestamp() - prevTimestamp
//                    logger.debug { "Delaying next record emission by ${delayMs.milliseconds}" }
//                    delay(delayMs) // nitpicking: should also consider the time we spend emitting
//                }
//                prevTimestamp = consumerRecord.timestamp()
//                emit(session.textMessage(consumerRecord.value()))
//            }
//        } else {
//            logger.debug { "Emitting kartchrono messages instantly" }
//            records.forEach { consumerRecord ->
//                logger.debug { "Got kafka record[offset=${consumerRecord.offset()}, value=$consumerRecord.value()}]" }
//                emit(session.textMessage(consumerRecord.value()))
//            }
//        }
//    }
//
//    private enum class SessionStage {
//        INIT, DISPENSE
//    }
}
