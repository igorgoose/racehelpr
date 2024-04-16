package com.igorgoose.racehelpr.scraper.kartchrono

import com.igorgoose.racehelpr.scraper.kartchrono.model.ApplyConfigurationRequest
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/sessions")
class KartchronoSessionController(
    private val sessionManager: KartchronoSessionManager
) {

    @PostMapping("/{sessionId}/config")
    suspend fun config(@PathVariable("sessionId") sessionId: String, @RequestBody request: ApplyConfigurationRequest) {
        sessionManager.applyConfiguration(sessionId, request)
    }

    @PostMapping("/{sessionId}/pause")
    suspend fun pause(@PathVariable("sessionId") sessionId: String) {
        sessionManager.pauseSession(sessionId)
    }

    @PostMapping("/{sessionId}/produce")
    suspend fun produce(@PathVariable("sessionId") sessionId: String, @RequestParam("count") count: Int = -1) {
        sessionManager.produce(sessionId, count)
    }
}