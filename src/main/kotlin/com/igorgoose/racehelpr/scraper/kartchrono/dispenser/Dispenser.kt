package com.igorgoose.racehelpr.scraper.kartchrono.dispenser

import org.springframework.web.reactive.socket.WebSocketMessage

interface Dispenser {
    suspend fun dispense()
    fun react(message: WebSocketMessage)
}

