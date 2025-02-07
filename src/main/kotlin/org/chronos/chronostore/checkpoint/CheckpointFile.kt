package org.chronos.chronostore.checkpoint

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.io.structure.ChronoStoreStructure
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.json.JsonUtil

data class CheckpointFile(
    val wallClockTime: Timestamp,
    val file: VirtualReadWriteFile,
    val checkpointData: CheckpointData,
) {
    fun delete() {
        this.file.deleteOverWriterFileIfExists()
        this.file.delete()
    }

    companion object {

        private val log = KotlinLogging.logger {}

        private val VirtualFile.wallClockTimeOrNull: Timestamp?
            get() {
                if (!name.startsWith(ChronoStoreStructure.CHECKPOINT_FILE_PREFIX)) {
                    return null
                }
                if (!name.endsWith(ChronoStoreStructure.CHECKPOINT_FILE_EXTENSION)) {
                    return null
                }
                val timestampAsString = name
                    .removePrefix(ChronoStoreStructure.CHECKPOINT_FILE_PREFIX)
                    .removeSuffix(ChronoStoreStructure.CHECKPOINT_FILE_EXTENSION)

                val timestamp = timestampAsString.toLongOrNull()
                    ?: return null

                return timestamp.takeIf { it > 0 }
            }


        fun readOrNull(file: VirtualReadWriteFile): CheckpointFile? {
            val wallClockTime = file.wallClockTimeOrNull
                ?: return null // not a checkpoint file
            return try {
                CheckpointFile(
                    wallClockTime = wallClockTime,
                    file = file,
                    checkpointData = file.withInputStream(JsonUtil::readJsonAsObject),
                )
            } catch (e: Exception) {
                log.warn(e) { "Checkpoint file '${file.path}' has been corrupted and will be ignored! Cause: ${e}" }
                null
            }
        }

        fun write(file: VirtualReadWriteFile, checkpointData: CheckpointData): CheckpointFile {
            val timestamp = file.wallClockTimeOrNull
                ?: throw IllegalArgumentException("The given file is not a valid checkpoint file: ${file.path}")
            file.deleteOverWriterFileIfExists()
            file.withOverWriter { overWriter ->
                JsonUtil.writeJson(checkpointData, overWriter.outputStream)
                overWriter.commit()
            }
            return CheckpointFile(timestamp, file, checkpointData)
        }

        fun createFileName(wallClockTime: Timestamp): String {
            require(wallClockTime >= 0) { "Precondition violation - argument 'wallClockTime' must not be negative!" }
            return "${ChronoStoreStructure.CHECKPOINT_FILE_PREFIX}${wallClockTime}${ChronoStoreStructure.CHECKPOINT_FILE_EXTENSION}"
        }

    }

}