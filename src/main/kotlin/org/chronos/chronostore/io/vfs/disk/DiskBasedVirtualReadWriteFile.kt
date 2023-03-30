package org.chronos.chronostore.io.vfs.disk

import mu.KotlinLogging
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.StandardCopyOption

class DiskBasedVirtualReadWriteFile(
    parent: DiskBasedVirtualDirectory?,
    file: File,
) : DiskBasedVirtualFile(parent, file), VirtualReadWriteFile {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    override fun create() {
        if(this.exists()){
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

    override fun createAppendOutputStream(): FileOutputStream {
        return FileOutputStream(file, true)
    }

    override fun createOverWriter(): VirtualReadWriteFile.OverWriter {
        val tempFile = File(file.path + ".tmp")
        if (tempFile.exists()) {
            throw IOException("temp path $tempFile already exists")
        }
        return FileOverWriter(tempFile, FileOutputStream(tempFile))
    }

    private inner class FileOverWriter(private val tempFile: File, private val stream: FileOutputStream) : VirtualReadWriteFile.OverWriter {

        private var inProgress = true

        override val outputStream: FileOutputStream
            get() {
                assertNotClosed()
                return stream
            }

        private fun assertNotClosed() {
            check(inProgress) { "FileOverWriter on '${this@DiskBasedVirtualReadWriteFile.file.absolutePath}' has already been closed!" }
        }

        override fun commit() {
            assertNotClosed()
            log.trace { "Committing writing $file" }
            inProgress = false
            try {
                tryClose()
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
                tryClose()
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

        private fun tryClose() {
            try {
                stream.close()
            } catch (e: IOException) {
                if (e.message != "Stream Closed") {
                    throw e
                }
            }
        }
    }

}