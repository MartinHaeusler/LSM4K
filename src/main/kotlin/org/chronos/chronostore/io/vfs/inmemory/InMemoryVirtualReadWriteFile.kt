package org.chronos.chronostore.io.vfs.inmemory

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.stream.UnclosableOutputStream.Companion.unclosable
import java.io.ByteArrayOutputStream
import java.io.OutputStream

class InMemoryVirtualReadWriteFile(
    parent: InMemoryVirtualDirectory?,
    fileSystem: InMemoryVirtualFileSystem,
    name: String,
) : InMemoryVirtualFile(parent, fileSystem, name), VirtualReadWriteFile {


    override fun <T> append(action: (OutputStream) -> T): T {
        if (!this.fileSystem.isExistingPath(this.path)) {
            this.fileSystem.createNewFile(this.path)
        }
        return this.fileSystem.openAppendOutputStream(this.path).use(action)
    }

    override fun create(): VirtualReadWriteFile {
        if (this.exists()) {
            return this
        }
        this.fileSystem.createNewFile(this.path)
        return this
    }

    override fun delete() {
        this.fileSystem.delete(this.path)
    }

    override fun deleteIfExists(): Boolean {
        return this.fileSystem.deleteIfExists(this.path)
    }

    override fun truncateAfter(bytesToKeep: Long) {
        require(bytesToKeep >= 0) { "Argument 'bytesToKeep' (${bytesToKeep}) must not be negative!" }
        check(this.exists()) { "Cannot truncate file '${this.path}' because it doesn't exist!" }
        if (bytesToKeep >= this.length) {
            // nothing to truncate
            return
        }
        this.fileSystem.truncateFile(this.path, bytesToKeep)
    }

    override fun createOverWriter(): VirtualReadWriteFile.OverWriter {
        return InMemoryOverWriter()
    }

    override fun deleteOverWriterFileIfExists() {
        // no-op
    }

    inner class InMemoryOverWriter : VirtualReadWriteFile.OverWriter {

        private var isOpen: Boolean = true

        private val stream = ByteArrayOutputStream()

        override val outputStream: OutputStream
            get() {
                assertIsOpen()
                // only expose unclosable variants of the stream to
                // the "outside world". We want to have control over
                // when this stream gets closed inside this class.
                return stream.unclosable()
            }

        private fun assertIsOpen() {
            if (!this.isOpen) {
                throw IllegalStateException("OverWriter on '${this@InMemoryVirtualReadWriteFile.path}' has already been closed!")
            }
        }

        override fun commit() {
            this.assertIsOpen()
            val file = this@InMemoryVirtualReadWriteFile
            this.outputStream.flush()
            this.stream.flush()
            if (!file.exists()) {
                file.parent?.mkdirs()
                file.create()
            }
            file.fileSystem.overwrite(file.path, Bytes.wrap(this.stream.toByteArray()))
            this.stream.close()
            this.isOpen = false
        }

        override fun rollback() {
            if (!this.isOpen) {
                return
            }
            this.stream.close()
            this.isOpen = false
        }

    }

}