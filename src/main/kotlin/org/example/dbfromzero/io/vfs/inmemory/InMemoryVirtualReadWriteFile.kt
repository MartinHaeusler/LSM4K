package org.example.dbfromzero.io.vfs.inmemory

import org.example.dbfromzero.io.vfs.VirtualReadWriteFile
import org.example.dbfromzero.util.Bytes
import java.io.ByteArrayOutputStream
import java.io.OutputStream

class InMemoryVirtualReadWriteFile(
    parent: InMemoryVirtualDirectory,
    name: String,
) : InMemoryVirtualFile(parent, name), VirtualReadWriteFile {

    override fun createAppendOutputStream(): OutputStream {
        return this.fileSystem.openAppendOutputStream(this.path)
    }

    override fun delete() {
        this.fileSystem.delete(this.path)
    }

    override fun createOverWriter(): VirtualReadWriteFile.OverWriter {
        return OverWriterImpl()
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
            file.fileSystem.overwrite(file.path, Bytes(this.outputStream.toByteArray()))
            this.outputStream.close()
            this.isOpen = false
        }

        override fun abort() {
            this.outputStream.close()
            this.isOpen = false
        }

    }

}