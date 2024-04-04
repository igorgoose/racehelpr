package com.igorgoose.racehelpr.scraper.label

import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.util.VolumeUtil
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean


@Component
class LabelManager(
    private val volumeUtil: VolumeUtil,
    private val objectMapper: ObjectMapper
) {
    companion object {
        private const val FILE = "labels.txt"
    }

    private val logger = KotlinLogging.logger { }
    private val flushing = AtomicBoolean(false)

    @Volatile
    private var buffer: ArrayList<Label> = ArrayList(50)

    @PreDestroy
    fun flushRemaining() {
        flush()
    }

    // most definitely not thread safe, however I'll let it slide
    fun saveLabel(label: Label) {
        logger.debug { "Saving label $label" }
        if (flushRequired()) flush()
        buffer.add(label)
    }

    private fun flushRequired(): Boolean = buffer.size >= 40

    /*
       using flushing field still does not guarantee that we can't write some more labels to bufferToFlush in saveLabel(),
       however the main goal is to avoid flushing same buffer twice
     */
    fun flush() {
        if (flushing.compareAndSet(false, true)) {
            logger.debug { "Flushing labels" }
            val bufferToFlush = buffer
            buffer = ArrayList(50)
            flushing.set(false)
            val flushString =
                bufferToFlush.joinToString(separator = "\n") { objectMapper.writeValueAsString(it) } + "\n"
            if (flushString.isNotBlank()) volumeUtil.write(FILE, flushString.toByteArray())
            else logger.debug { "No labels to flush" }
        }
    }

    fun truncateLabels() {
        volumeUtil.truncate(FILE)
    }
}