package com.igorgoose.racehelpr.scraper.util

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.*

@Component
class VolumeUtil(
    @Value("\${scraper.volume}") volumePathString: String,
) {
    private val volumePath = Path.of(volumePathString)
    private val logger = KotlinLogging.logger {  }

    @PostConstruct
    private fun ensureVolumeExists() {
        if (!Files.isDirectory(volumePath)) error("volume path is not a dir($volumePath)")
    }

    fun write(volumeFile: String, data: ByteArray, vararg options: OpenOption = arrayOf(StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
        val filePath = volumePath.resolve(volumeFile)
        filePath.writeBytes(data, *options)
    }

    fun readString(volumeFile: String, vararg options: OpenOption = arrayOf(StandardOpenOption.READ, StandardOpenOption.CREATE)): ReadResult<String> {
        val filePath = volumePath.resolve(volumeFile)
        if (!filePath.exists()) {
            filePath.createFile()
            return NoData()
        } else if (!filePath.isRegularFile()) {
            error("$volumeFile is not a regular file")
        }
        if (filePath.fileSize() == 0L) return NoData()
        return Data(filePath.readText())
    }

    fun truncate(volumeFile: String) {
        val filePath = volumePath.resolve(volumeFile)
        return filePath.deleteIfExists().let { deleted ->
            if (deleted) logger.info { "Truncated volume file $volumeFile" }
            else logger.info { "Did not truncate volume file $volumeFile, as it does not exist" }
        }
    }

    sealed interface ReadResult<T>

    class NoData<T> : ReadResult<T>
    class Data<T>(val data: T) : ReadResult<T>
}