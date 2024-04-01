package com.igorgoose.racehelpr.schedule

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.io.path.fileSize
import kotlin.math.max
import kotlin.math.min

@Component
class ScraperScheduleManager(
    @Value("\${scheduler.volume}") private val volumePath: String,
    private val objectMapper: ObjectMapper
) {
    private val logger = KotlinLogging.logger {}
    private var tasks: ArrayList<ScraperTask> = loadSchedule()

    @Volatile
    private var flushNeeded = false

    // WARNING: not thread safe
    fun schedule(task: ScraperTask): List<ScraperTask> {
        tasks = mergeTasks(task, tasks)
        flushNeeded = true
        return tasks
    }

    private fun mergeTasks(newTask: ScraperTask, tasks: ArrayList<ScraperTask>): ArrayList<ScraperTask> {
        val newTasks = ArrayList<ScraperTask>()
        var i = 0
        while (i < tasks.size && newTask.start > tasks[i].stop) newTasks += tasks[i++]

        if (i == tasks.size) {
            newTasks += newTask
            return newTasks
        }

        val newLabels = TreeMap<Long, List<String>>()
        newLabels.putAll(tasks[i].labelsByTime)
        newLabels.putAll(newTask.labelsByTime)

        val newStart = min(newTask.start, tasks[i].start)
        while (i < tasks.size && newTask.stop < tasks[i].start) {
            newLabels.putAll(tasks[i].labelsByTime)
            i++
        }
        val newStop = max(newTask.stop, tasks[i - 1].stop)

        newTasks += ScraperTask(newStart, newStop, newLabels)
        while (i < tasks.size) newTasks += tasks[i]
        return newTasks
    }

    private fun loadSchedule(): ArrayList<ScraperTask> {
        val volumePath = Path.of(volumePath)
        if (!Files.isDirectory(volumePath)) error("volume path is not a dir($volumePath)")
        val filePath = volumePath.resolve("schedule.json")

        if (!Files.exists(filePath)) {
            logger.debug { "Scheduler data file does not exist. Creating one..." }
            Files.createFile(filePath)
            return arrayListOf()
        }

        logger.debug { "Scheduler data file exists. Reading data..." }

        if (filePath.fileSize() == 0L) return arrayListOf<ScraperTask>().also {
            logger.debug { "Scheduler data file is empty" }
        }
        return objectMapper.readValue(filePath.toFile(), object : TypeReference<ArrayList<ScraperTask>>() {})
    }
}

