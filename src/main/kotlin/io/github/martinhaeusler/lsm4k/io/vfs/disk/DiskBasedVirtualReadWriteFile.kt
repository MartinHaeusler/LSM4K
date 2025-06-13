package io.github.martinhaeusler.lsm4k.io.vfs.disk

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.util.stream.UnclosableOutputStream.Companion.unclosable
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption

class DiskBasedVirtualReadWriteFile(
    parent: DiskBasedVirtualDirectory?,
    file: File,
    vfs: DiskBasedVirtualFileSystem,
) : DiskBasedVirtualFile(parent, file, vfs), VirtualReadWriteFile {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    val overwriteFile = File(file.path + ".tmp")

    override fun create(): VirtualReadWriteFile {
        if (this.exists()) {
            return this
        }
        this.parent?.mkdirs()
        this.file.createNewFile()
        return this
    }

    override fun delete() {
        val deleted = file.delete()
        if (!deleted) {
            throw IOException("Failed to delete $file")
        }
    }

    override fun deleteIfExists(): Boolean {
        return Files.deleteIfExists(this.file.toPath())
    }

    override fun <T> append(action: (OutputStream) -> T): T {
        return this.vfs.settings.fileSyncMode.writeAppend(this.file, action)
    }

    override fun createOverWriter(): VirtualReadWriteFile.OverWriter {
        if (this.overwriteFile.exists()) {
            throw IOException("File Overwrite temp path '${this.overwriteFile.absolutePath}' already exists")
        }
        return FileOverWriter(this.overwriteFile, this.vfs.settings)
    }

    override fun deleteOverWriterFileIfExists() {
        val tempFile = File(file.path + ".tmp")
        Files.deleteIfExists(tempFile.toPath())
    }

    override fun truncateAfter(bytesToKeep: Long) {
        require(bytesToKeep >= 0) { "Argument 'bytesToKeep' (${bytesToKeep}) must not be negative!" }
        check(this.exists()) { "Cannot truncate file '${this.path}' because it doesn't exist!" }
        if (bytesToKeep >= this.length) {
            // nothing to truncate
            return
        }
        FileOutputStream(this.file, true).use { fos ->
            fos.channel.use { channel ->
                channel.truncate(bytesToKeep)
            }
        }
    }

    private inner class FileOverWriter(
        private val tempFile: File,
        private val settings: DiskBasedVirtualFileSystemSettings,
    ) : VirtualReadWriteFile.OverWriter {

        private var inProgress = true

        private val internalOutputStream: OutputStream = this.settings.fileSyncMode.createOutputStream(
            target = this.tempFile,
            append = false
        )

        override val outputStream: OutputStream
            get() {
                assertNotClosed()
                // only expose unclosable variants of the stream to
                // the "outside world". We want to have control over
                // when this stream gets closed inside this class.
                return internalOutputStream.unclosable()
            }

        private fun assertNotClosed() {
            check(inProgress) { "FileOverWriter on '${this@DiskBasedVirtualReadWriteFile.file.absolutePath}' has already been closed!" }
        }

        override fun commit() {
            assertNotClosed()
            log.trace { "Committing writing $file" }
            inProgress = false
            try {
                this.internalOutputStream.flush()
                this.internalOutputStream.close()
                Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
                // fsync the parent directory (this is needed primarily on linux)
                parent?.fsync()
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
                this.internalOutputStream.close()
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