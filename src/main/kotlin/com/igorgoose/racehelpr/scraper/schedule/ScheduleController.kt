package com.igorgoose.racehelpr.scraper.schedule

import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/schedule")
class ScheduleController(private val scheduleManager: ScraperScheduleManager) {

    @GetMapping
    fun getState(): TasksState {
        return scheduleManager.getState()
    }

    @PostMapping
    fun schedule(@RequestBody request: ScheduleRequest): TasksState {
        return scheduleManager.schedule(request)
    }

    @DeleteMapping("/{id}")
    fun cancel(@PathVariable id: Int): TasksState {
        return scheduleManager.cancelTask(id)
    }
}