package com.igorgoose.racehelpr.scraper.schedule

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.Instant
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference

data class ScheduleRequest(
    val label: String,
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    val start: Instant,
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    val stop: Instant
)

data class TasksState(
    val tasks: List<ScraperTask>
)

data class ScraperTask(
    val id: Int,
    // timestamps in seconds
    var start: Instant,
    var stop: Instant,
    // time is between start and stop inclusive
    var labelsByTime: ConcurrentNavigableMap<Instant, List<String>>,
    var state: AtomicReference<State> = AtomicReference(State.NEW)
) {
    init {
        require(labelsByTime.all { (time, _) -> time in (start..stop) }) {
            "Time for each label must lie between $start and $stop(labelsByTime=$labelsByTime)"
        }
    }

    enum class State {
        NEW, EXECUTING, CANCELLED
    }
}

fun ScheduleRequest.toScraperTask(id: Int) = ScraperTask(
    id,
    start,
    stop,
    ConcurrentSkipListMap<Instant, List<String>>().apply { put(start, listOf(label)) }
)