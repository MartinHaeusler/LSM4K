package org.chronos.chronostore.io.vfs.disk

import mu.KotlinLogging
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.sync
import org.chronos.chronostore.util.stream.UnclosableOutputStream.Companion.unclosable
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import kotlin.math.min

class DiskBasedVirtualReadWriteFile(
    parent: DiskBasedVirtualDirectory?,
    file: File,
) : DiskBasedVirtualFile(parent, file), VirtualReadWriteFile {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    val overwriteFile = File(file.path + ".tmp")

    override fun create() {
        if (this.exists()) {
            return
        }
        this.parent?.mkdirs()
        this.file.createNewFile()

    }

    override fun delete() {
        val deleted = file.delete()
        if (!deleted) {
            throw IOException("Failed to delete $file")
        }
    }

    override fun <T> append(action: (OutputStream) -> T): T {
        val outputStream = FileOutputStream(file, true)
        try {
            val bufferedOutputStream = outputStream.unclosable().buffered()
            val result = action(bufferedOutputStream)
            bufferedOutputStream.flush()
            return result
        } finally {
            outputStream.flush()
            outputStream.sync(file)
            outputStream.close()
        }
    }

    override fun createOverWriter(): VirtualReadWriteFile.OverWriter {
        if (this.overwriteFile.exists()) {
            throw IOException("File Overwrite temp path '${this.overwriteFile.absolutePath}' already exists")
        }
        return FileOverWriter(this.overwriteFile)
    }

    override fun deleteOverWriterFileIfExists() {
        val tempFile = File(file.path + ".tmp")
        Files.deleteIfExists(tempFile.toPath())
    }

    override fun truncateAfter(bytesToKeep: Long) {
        require(bytesToKeep >= 0) { "Argument 'bytesToKeep' (${bytesToKeep}) must not be negative!" }
        check(this.exists()) { "Cannot truncate file '${this.path}' because it doesn't exist!" }
        if(bytesToKeep >= this.length){
            // nothing to truncate
            return
        }
        FileOutputStream(this.file, true).use { fos ->
            fos.channel.use { channel ->
                channel.truncate(bytesToKeep)
            }
        }
    }

    private inner class FileOverWriter(private val tempFile: File) : VirtualReadWriteFile.OverWriter {

        private var inProgress = true

        private val fileOutputStream: FileOutputStream = this.tempFile.outputStream()
        private val stream = fileOutputStream.buffered()

        override val outputStream: OutputStream
            get() {
                assertNotClosed()
                // only expose unclosable variants of the stream to
                // the "outside world". We want to have control over
                // when this stream gets closed inside this class.
                return stream.unclosable()
            }

        private fun assertNotClosed() {
            check(inProgress) { "FileOverWriter on '${this@DiskBasedVirtualReadWriteFile.file.absolutePath}' has already been closed!" }
        }

        override fun commit() {
            assertNotClosed()
            log.trace { "Committing writing $file" }
            inProgress = false
            try {
                this.stream.flush()
                this.fileOutputStream.flush()
                this.fileOutputStream.sync(this.tempFile)
                this.stream.close()
                this.fileOutputStream.close()
                Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch (e: IOException) {
                rollback()
                throw e
            }
        }

        override fun rollback() {
            if (!inProgress) {
                return
            }
            inProgress = false
            log.trace { "Aborting writing $file" }
            try {
                this.stream.close()
            } catch (e: Exception) {
                log.warn(e) { "Ignoring error in closing file" }
            } finally {
                try {
                    val deleted = tempFile.delete()
                    if (deleted) {
                        log.trace { "Deleted temporary path $tempFile" }
                    } else {
                        log.warn {
                            "Failed to deleted temporary path $tempFile. Will have to be removed manually"
                        }
                    }
                } catch (e: Exception) {
                    log.warn(e) { "Error in deleting temporary path '${tempFile}' " }
                }
            }
        }

    }

}