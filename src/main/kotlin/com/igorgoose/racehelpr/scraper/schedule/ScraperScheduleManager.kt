package com.igorgoose.racehelpr.scraper.schedule

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.util.VolumeUtil
import com.igorgoose.racehelpr.scraper.websocket.WebSocketHandlerFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.net.URI
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.math.max
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * For now this is a simple thread unsafe implementation that will break if you schedule a task while another task
 * is running. For now this is sufficient as I just want to scrape some data and schedule a couple scraping sessions.
 *
 *
 * TODO actually might not be implemented as this can be managed manually by the user(developer)
 * Task Execution
 *
 * The task listens to a websocket connection, writes the messages we get to kafka, and records the offset of the first
 * written message to a file with a label, so we can reference that offset through that label later. To reduce the number of
 * tasks created we merge tasks together if they share time of execution. For example if task1 starts at 1 and stops
 * at 5(from now on I will refer to start and stop using the following notation (1, 5)) and task2 (3, 7) we can create
 * a single task (1, 7) and store labels (1: label1, 3: label2).
 *
 * Task Scheduling
 *
 * We should be cautious while scheduling new tasks as it might happen during the execution process or when a task is scheduled.
 * 1. newTask.start > currentTask.stop: nothing bad happens, as it does not change the scheduled task or the task being executed
 * 2. newTask.start < currentTask.start
 *
 *      2.1 currentTask is being executed: an error because newTask.start < current time
 *
 *      2.2 currentTask is scheduled: we need to reschedule if possible. The problem is that current task might start
 *      executing during rescheduling, which we need to avoid. Also, if newTask.start time is reached while scheduling
 *      we might miss some labels that happen earlier than the moment task starts executing. In such case we will mark the
 *      first message we get with all the labels we've gone past.
 *
 * 3. newTask.start >= currentTask.start && newTask.start <= currentTask.stop
 *
 *      3.1 currentTask is being executed. Immediately check that newTask.start > current time. Still can reach newTask.start
 *      while updating the current task. In such case we will mark the next message we get with all labels we've gone past.
 *
 *      3.2 current task is scheduled. Still the same problem: the task can get started and go past some labels. The same
 *      solution.
 *
 * 4. newTask.stop <= currentTask.stop: nothing happens in either case as we don't modify currentTask.stop
 * 5. newTask.stop > currentTask.stop: we need to postpone the end of the task.
 *
 *      5.1 currentTask is being executed or is scheduled. In either case the concern is that currentTask can complete before
 *      we change currentTask.stop, then we will lose the time span between currentTask.stop and newTask.stop. In such case
 *      there are several solutions:
 *
 *      - forbid task completion, while the task continues writing messages to kafka. modify currentTask.stop,
 *        allow task completion.
 *      - same, but instead of writing messages directly to kafka, we put them in a buffer, and after allowing task
 *      completion we write only the ones that were received before currentTask.stop
 *
 */
@Component
class ScraperScheduleManager(
    @Value("\${scraper.kartchrono-url}") private val kartchronoUrl: String,
    private val volumeUtil: VolumeUtil,
    private val webSocketHandlerFactory: WebSocketHandlerFactory,
    private val webSocketClient: WebSocketClient,
    private val objectMapper: ObjectMapper
) {
    companion object {
        private const val FILE = "schedule.json"
    }

    private val idCounter = AtomicInteger(0)
    private val tasksRwl: ReadWriteLock = ReentrantReadWriteLock()
    private val wLock: Lock = tasksRwl.writeLock()
    private val rLock: Lock = tasksRwl.readLock()
    private val logger = KotlinLogging.logger {}
    private val scheduledTasks: ArrayList<ScraperTask> = loadSchedule()

    @PostConstruct
    fun scheduleOnStart() {
        scheduledTasks.forEach {
            scheduleTask(it)
        }
    }

    @PreDestroy
    fun saveTasks() {
        val bytes = read { objectMapper.writeValueAsBytes(scheduledTasks) }
        logger.info { "Saving tasks to $FILE" }
        volumeUtil.write(
            FILE,
            bytes,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
        )
    }

    fun schedule(scheduleRequest: ScheduleRequest): TasksState {
        val task = scheduleRequest.toScraperTask(idCounter.getAndIncrement())
        scheduleTask(task)
        insertTask(task)
        saveTasks()
        return getState()
    }

    fun cancelTask(id: Int): TasksState {
        val task: ScraperTask = read { scheduledTasks.find { it.id == id } ?: error("no task with id $id") }
        val state = task.state.compareAndExchange(ScraperTask.State.NEW, ScraperTask.State.CANCELLED)
        if (state != ScraperTask.State.NEW) {
            logger.info { "Could not cancel task $task, as the state was $state" }
            throw HttpClientErrorException(HttpStatus.BAD_REQUEST, "Task is either cancelled or in progress")
        }

        logger.info { "Removing cancelled task $task" }
        write { scheduledTasks.remove(task) }
        saveTasks()
        return getState()
    }

    private fun loadSchedule(): ArrayList<ScraperTask> {
         return when (val readResult = volumeUtil.readString(FILE)) {
            is VolumeUtil.Data<String> -> objectMapper.readValue(
                readResult.data,
                object : TypeReference<ArrayList<ScraperTask>>() {})

            is VolumeUtil.NoData<String> -> {
                logger.debug { "No data found in $FILE" }
                arrayListOf()
            }
        }.also {
            val maxId = read { it.fold(0) { acc, task ->
                max(acc, task.id)
            } }
            idCounter.set(maxId + 1)
        }
    }

    private fun scheduleTask(task: ScraperTask) {
        logger.info { "Scheduling task $task" }
        val startDelay = max(task.start.epochSecond - Instant.now().epochSecond, 0L).seconds
        logger.debug { "Delaying task start by $startDelay" }
        Mono.delay(startDelay.toJavaDuration())
            .then(
                Mono.fromRunnable<Unit> {
                    val state = task.state.compareAndExchange(ScraperTask.State.NEW, ScraperTask.State.EXECUTING)
                    if (state != ScraperTask.State.NEW) {
                        logger.info { "Not executing task $task, as the state is $state" }
                        return@fromRunnable
                    }

                    logger.info { "Starting task $task" }
                    webSocketClient
                        .execute(URI.create(kartchronoUrl), webSocketHandlerFactory.createScraper(task))
                        .then(Mono.fromRunnable<Void> {
                            logger.debug { "Removing task $task" }
                            write { scheduledTasks.remove(task) }
                            logger.debug { "Removed task $task from scheduled tasks" }
                            saveTasks()
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe()
                }
            )
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe()
    }

    fun getState(): TasksState {
        return TasksState(scheduledTasks)
    }

    private fun insertTask(task: ScraperTask) {
        var l = 0

        read {
            var r = scheduledTasks.size
            while (l < r) {
                val mid: Int = (l + r) / 2
                if (scheduledTasks[mid].start > task.start) {
                    r = mid
                } else {
                    l = mid + 1
                }
            }
        }
        write { scheduledTasks.add(l, task) }
    }

    private inline fun <T> read(readBlock: () -> T): T {
        rLock.lock()
        try {
            return readBlock()
        } finally {
            rLock.unlock()
        }
    }

    private inline fun <T> write(writeBlock: () -> T): T {
        wLock.lock()
        try {
            return writeBlock()
        } finally {
            wLock.unlock()
        }
    }
}
