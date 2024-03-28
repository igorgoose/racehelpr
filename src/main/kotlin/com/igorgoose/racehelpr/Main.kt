package com.igorgoose.racehelpr

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.web.socket.*
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import java.util.*

const val TOPIC = "raw_chrono_messages"

fun main() {
    val url = System.getenv("KART_CHRONO_URL")
    val client = StandardWebSocketClient()

    createTopicsIfNotExist(listOf(TOPIC), false)

    createProducer().use { producer ->
        client.execute(WsHandler(producer), url)
            .thenApply {
                it.sendMessage(TextMessage("{\"trackId\":\"68dddfb7cbe7ba1861db45bff7bdd308\"}"))
            }

        Scanner(System.`in`).nextLine()
    }
}

class WsHandler(private val producer: KafkaProducer<Int, String>) : WebSocketHandler {
    override fun afterConnectionEstablished(session: WebSocketSession) {
        println("connection established[session=$session]")
    }

    override fun handleMessage(session: WebSocketSession, message: WebSocketMessage<*>) {
        println("received message[payload=${message.payload}, len=${message.payloadLength}, last=${message.isLast}]")
        message.payload.let {
            if (it is String) producer.send(ProducerRecord(TOPIC, it)) { meta, error ->
                println("sent message to kafka[meta=$meta, error=$error]")
            }
        }
    }

    override fun handleTransportError(session: WebSocketSession, exception: Throwable) {
        System.err.println("transport error: ${exception.message}")
    }

    override fun afterConnectionClosed(session: WebSocketSession, closeStatus: CloseStatus) {
        println("connection closed[status=$closeStatus]")
    }

    override fun supportsPartialMessages(): Boolean = false
}

