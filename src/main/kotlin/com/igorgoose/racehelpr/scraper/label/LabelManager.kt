package com.igorgoose.racehelpr.scraper.label

import com.fasterxml.jackson.databind.ObjectMapper
import com.igorgoose.racehelpr.scraper.util.VolumeUtil
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Component


@Component
class LabelManager(
    private val volumeUtil: VolumeUtil,
    private val objectMapper: ObjectMapper
) {
    companion object {
        private const val FILE = "labels.txt"
    }

    private val logger = KotlinLogging.logger { }
    private val cache: MutableList<Label> = loadLabels()

    fun getAllLabels(): List<Label> = cache

    fun getByValue(value: String): Label? = cache.find { it.value == value }

    // most definitely not thread safe, however I'll let it slide
    fun saveLabel(label: Label) {
        logger.debug { "Saving label $label" }
        val flushString = objectMapper.writeValueAsString(label) + "\n"
        volumeUtil.write(FILE, flushString.toByteArray())
        cache.add(label)
    }

    fun truncateLabels() {
        volumeUtil.truncate(FILE)
    }

    private final fun loadLabels(): MutableList<Label> {
        return when (val result = volumeUtil.readString(FILE)) {
            is VolumeUtil.Data<String> -> result.data.split("\n")
                .filter { it.isNotBlank() }
                .map { objectMapper.readValue(it, Label::class.java) }
                .toMutableList()

            else -> mutableListOf()
        }
    }
}