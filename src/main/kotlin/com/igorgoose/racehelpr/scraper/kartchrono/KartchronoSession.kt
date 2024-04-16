package com.igorgoose.racehelpr.scraper.kartchrono

import com.igorgoose.racehelpr.scraper.kafka.KafkaConsumerUtils
import com.igorgoose.racehelpr.scraper.kartchrono.model.ApplyConfigurationRequest
import com.igorgoose.racehelpr.scraper.label.LabelManager
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.selects.select
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import java.util.concurrent.locks.ReentrantLock
import kotlin.coroutines.CoroutineContext

class KartchronoSession(
    private val session: WebSocketSession,
    private val labelManager: LabelManager,
    private val kafkaConsumerUtils: KafkaConsumerUtils,
    context: CoroutineContext,
    initConsumer: () -> KafkaConsumer<Int?, String>
) : AutoCloseable, CoroutineScope {
    companion object {
        private val logger = KotlinLogging.logger {}
    }
    override val coroutineContext: CoroutineContext = SupervisorJob() + context

    private val consumer: KafkaConsumer<Int?, String> by lazy(initConsumer)
    private val configChannel = Channel<KartchronoSessionConfiguration>()
    private val commandChannel = Channel<Command>()
    private val emissionChannel = Channel<ConsumerRecord<Int?, String>>(1000, BufferOverflow.SUSPEND)
    private val producerLock: ReentrantLock = ReentrantLock()
    @Volatile
    private var producerJob: Job? = null

    suspend fun start(collector: FlowCollector<WebSocketMessage>) {
        log("Awaiting configuration", logger::debug)

        val config = configChannel.receive()
        kafkaConsumerUtils.setOffset(consumer, config.offset)

        log("Received configuration $config, producing messages", logger::debug)
        doProduce(-1)

        while (isActive) {
            select {
                commandChannel.onReceive {
                    react(it)
                }
                emissionChannel.onReceive { record ->
                    collector.emit(session.textMessage(record.value()))
                }
            }
        }
    }

    suspend fun pause() {
        log("Queueing pause command", logger::debug)
        commandChannel.send(Pause)
        log("Queued pause command", logger::debug)
    }

    private suspend fun doPause() {
        if (producerJob != null) {
            log("Acquiring lock to pause producer", logger::debug)
            producerLock.lock()
            log("Acquired lock to pause producer", logger::debug)
            try {
                if (producerJob != null) {
                    log("Cancelling current producer job", logger::debug)
                    producerLock.unlock()
                    producerJob!!.cancelAndJoin()

                    producerLock.lock()
                    log("Cancelled current producer job", logger::debug)
                    producerJob = null
                    log("Producer paused", logger::info)
                } else {
                    log("No producer to pause", logger::debug)
                }
            } finally {
                log("Releasing producer job lock", logger::debug)
                producerLock.unlock()
            }
        } else {
            log("No producer to pause", logger::debug)
        }
    }

    suspend fun produce(count: Int = -1) {
        log("Queuing produce command", logger::debug)
        commandChannel.send(Produce(count))
        log("Queued produce command", logger::debug)
    }

    private fun doProduce(count: Int) {
        log("Creating producer to produce $count messages", logger::debug)
        createProducerJob {
            var cnt = count
            while (cnt == -1 || cnt > 0) {
                log("producing", logger::info)
                if (count > 0) cnt--
                delay(1000)
            }
        }
    }

    fun skipTo(offset: Int) {

    }

    fun skipTo(label: String) {

    }

    private suspend fun react(command: Command) {
        log("Reacting to command $command", logger::debug)
        when (command) {
            is Pause -> doPause()
            is Produce -> doProduce(command.count)
        }
    }

    suspend fun applyConfiguration(request: ApplyConfigurationRequest) {
        configChannel.send(request.toConfig())
    }

    private fun createProducerJob(block: suspend CoroutineScope.() -> Unit) = launch {
        producerLock.lock()
        try {
            if (producerJob == null) {
                log("Creating new producer: no currently active producer, just creating new one", logger::debug)
                producerJob = launch(block = block).also {
                    it.invokeOnCompletion { removeProducerJob() }
                }
            } else {
                log(
                    "Creating new producer: found currently active producer, cancelling old one and replacing it",
                    logger::debug
                )

                producerLock.unlock()
                producerJob!!.cancelAndJoin()

                producerLock.lock()
                producerJob = launch(block = block).also {
                    it.invokeOnCompletion { removeProducerJob() }
                }
            }
        } finally {
            log("Unlocking producer job lock", logger::debug)
            try {
                producerLock.unlock()
            } catch (e: IllegalMonitorStateException) {
                log("Attempt to unlock foreign lock", logger::warn)
            }
        }
    }

    private fun removeProducerJob() {
        producerLock.lock()
        try {
            producerJob = null
        } finally {
            producerLock.unlock()
        }
    }

    private fun ApplyConfigurationRequest.toConfig() = KartchronoSessionConfiguration(getOffset())

    private fun ApplyConfigurationRequest.getOffset(): Long = label?.let {
        labelManager.getByValue(it)?.offset ?: error("No label '$it'")
    } ?: offset ?: error("Either offset or label must be present in request[request=$this]")

    override fun close() {
        try {
            configChannel.close()
        } catch (e: Throwable) {
            log("Error while closing config channel", e, logger::error)
        }

        try {
            commandChannel.close()
        } catch (e: Throwable) {
            log("Error while closing command channel", e, logger::error)
        }

        try {
            emissionChannel.close()
        } catch (e: Throwable) {
            log("Error while closing emission channel", e, logger::error)
        }

        try {
            consumer.close()
        } catch (e: Throwable) {
            log("Error while closing consumer", e, logger::error)
        }
    }

    private fun log(message: String, func: (() -> Any?) -> Unit) {
        func {
            "$message[sessionId=${session.id}]"
        }
    }

    private fun log(message: String, exception: Throwable, func: (e: Throwable, () -> Any?) -> Unit) {
        func(exception) {
            "$message[sessionId=${session.id}]"
        }
    }
}

data class KartchronoSessionConfiguration(
    val offset: Long
)

private sealed interface Command
private data object Pause : Command
private data class Produce(val count: Int) : Command
