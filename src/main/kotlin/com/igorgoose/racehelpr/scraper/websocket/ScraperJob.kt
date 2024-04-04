package com.igorgoose.racehelpr.scraper.websocket

import com.igorgoose.racehelpr.scraper.label.Label
import com.igorgoose.racehelpr.scraper.label.LabelManager
import com.igorgoose.racehelpr.scraper.schedule.ScraperTask
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration


class ScraperJob(
    private val producer: KafkaProducer<Int, String>,
    private val scraperTopic: String,
    private val labelManager: LabelManager,
    private val task: ScraperTask
) : WebSocketHandler {
    private val logger = KotlinLogging.logger {}

    override fun handle(session: WebSocketSession): Mono<Void> {
        val receive = session.receive()
            .take((task.stop.toEpochMilli() - Instant.now().toEpochMilli()).milliseconds.toJavaDuration())
            .doOnNext { message ->
                if (message.type == WebSocketMessage.Type.TEXT) {
                    processMessage(message.payloadAsText)
                }
            }
            .then(
                Mono.fromRunnable<Void> {
                    logger.debug { "Task $task has stopped" }
                }
            )

        session
            .send(Mono.just(session.textMessage("{\"trackId\":\"68dddfb7cbe7ba1861db45bff7bdd308\"}")))
            .then(Mono.fromRunnable<Void> {
                logger.debug { "Sent track id to Kartchrono server" }
            })
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe()

        return receive.then()
    }

    private fun processMessage(message: String) {
        producer.send(ProducerRecord(scraperTopic, message)) { meta, error ->
            logger.debug { "Sent message to kafka[meta=$meta, error=$error]" }

            task.labelsByTime.headMap(Instant.now(), true).let { headMap ->
                headMap.values.flatten().forEach {
                    labelManager.saveLabel(Label(meta.offset(), it))
                }
                headMap.keys.iterator().let { iter ->
                    while(iter.hasNext()) {
                        iter.next()
                        iter.remove()
                    }
                }
            }
        }
    }
}