package com.igorgoose.racehelpr.schedule

import java.time.Instant
import java.util.*

data class ScheduleRequest(
    val label: String,
    val startTimestamp: Instant,
    val stopTimestamp: Instant
)

data class ScraperTask(
    // timestamps in seconds
    val start: Long,
    val stop: Long,
    // time is between start and stop inclusive
    val labelsByTime: TreeMap<Long, List<String>>
) {
    init {
        require(labelsByTime.all { (time, _) -> time in (start..stop) }) {
            "Time for each label must lie between $start and $stop(labelsByTime=$labelsByTime)"
        }
    }
}

fun ScheduleRequest.toScraperTask() = ScraperTask(
    startTimestamp.epochSecond,
    stopTimestamp.epochSecond,
    TreeMap<Long, List<String>>().apply { put(startTimestamp.epochSecond, listOf(label)) }
)