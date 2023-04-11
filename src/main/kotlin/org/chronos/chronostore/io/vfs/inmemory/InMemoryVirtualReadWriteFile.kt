package org.chronos.chronostore.io.vfs.inmemory

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.Bytes
import java.io.ByteArrayOutputStream
import java.io.OutputStream

class InMemoryVirtualReadWriteFile(
    parent: InMemoryVirtualDirectory?,
    fileSystem: InMemoryVirtualFileSystem,
    name: String,
) : InMemoryVirtualFile(parent, fileSystem, name), VirtualReadWriteFile {


    override fun <T> append(action: (OutputStream) -> T): T {
        if(!this.fileSystem.isExistingPath(this.path)){
            this.fileSystem.createNewFile(this.path)
        }
        return this.fileSystem.openAppendOutputStream(this.path).use(action)
    }

    override fun create() {
        if (this.exists()) {
            return
        }
        this.fileSystem.createNewFile(this.path)
    }

    override fun delete() {
        this.fileSystem.delete(this.path)
    }

    override fun createOverWriter(): VirtualReadWriteFile.OverWriter {
        return OverWriterImpl()
    }

    override fun deleteOverWriterFileIfExists() {
        // no-op
    }

    inner class OverWriterImpl : VirtualReadWriteFile.OverWriter {

        private var isOpen: Boolean = true
        override val outputStream: ByteArrayOutputStream = ByteArrayOutputStream()
            get() {
                assertIsOpen()
                return field
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
            if (!file.exists()) {
                file.parent?.mkdirs()
                file.create()
            }
            file.fileSystem.overwrite(file.path, Bytes(this.outputStream.toByteArray()))
            this.outputStream.close()
            this.isOpen = false
        }

        override fun rollback() {
            if (!this.isOpen) {
                return
            }
            this.outputStream.close()
            this.isOpen = false
        }

    }

}