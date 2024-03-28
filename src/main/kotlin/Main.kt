package com.igorgoose

import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketHandler
import org.springframework.web.socket.WebSocketMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import java.util.*

fun main() {
    val url = System.getenv("KART_CHRONO_URL")

    val client = StandardWebSocketClient()
    client.execute(WsHandler, url)
       .thenApply {
           it.sendMessage(TextMessage("{\"trackId\":\"68dddfb7cbe7ba1861db45bff7bdd308\"}"))
       }




    Scanner(System.`in`).nextLine()
}

object WsHandler: WebSocketHandler {
    override fun afterConnectionEstablished(session: WebSocketSession) {
        println("connection established[session=$session]")
    }

    override fun handleMessage(session: WebSocketSession, message: WebSocketMessage<*>) {
        println("received message[payload=${message.payload}]")
    }

    override fun handleTransportError(session: WebSocketSession, exception: Throwable) {
        System.err.println("transport error: ${exception.message}")
    }

    override fun afterConnectionClosed(session: WebSocketSession, closeStatus: CloseStatus) {
        println("connection closed[status=$closeStatus]")
    }

    override fun supportsPartialMessages(): Boolean = false
}

