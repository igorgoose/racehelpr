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
    suspend fun produce(
        @PathVariable("sessionId") sessionId: String,
        @RequestParam("count", required = false) count: Int = -1,
        @RequestParam("label", required = false) label: String? = null,
        @RequestParam("offset", required = false) offset: Long? = null,
        @RequestParam("ff", required = false) fastForward: Boolean = false
    ) {
        if (label != null) {
            if (fastForward) sessionManager.fastForwardToLabel(sessionId, label)
            else sessionManager.produceToLabel(sessionId, label)
        } else if (offset != null) {
            if (fastForward) sessionManager.fastForwardToOffset(sessionId, offset)
            else sessionManager.produceToOffset(sessionId, offset)
        } else {
            if (fastForward) sessionManager.fastForward(sessionId, count)
            else sessionManager.produce(sessionId, count)
        }
    }

    @PostMapping("/{sessionId}/assign-label")
    suspend fun setLabel(@PathVariable("sessionId") sessionId: String, @RequestParam label: String) {
        sessionManager.setLabel(sessionId, label)
    }

    @PostMapping("/{sessionId}/slowdown")
    suspend fun setLabel(@PathVariable("sessionId") sessionId: String, @RequestParam coef: Float) {
        sessionManager.slowDown(sessionId, coef)
    }
}