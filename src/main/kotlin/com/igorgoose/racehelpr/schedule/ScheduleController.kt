package com.igorgoose.racehelpr.schedule

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux

@RestController
class ScheduleController(private val scheduleManager: ScraperScheduleManager) {

    @PostMapping("/schedule")
    fun schedule(@RequestBody request: ScheduleRequest): List<ScraperTask> {
        return scheduleManager.schedule(request.toScraperTask())
    }
}