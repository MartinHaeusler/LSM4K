package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFile
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.util.Bytes
import java.io.File
import java.io.RandomAccessFile

/**
 * A [RandomFileAccessDriver] based on a [RandomAccessFile].
 *
 * This technique uses memory-mapped files.
 *
 * **ATTENTION:**
 * Memory-mapped files in Java have a maximum size of 2GB (due to API restrictions).
 * Files larger than this size will be *rejected* by this class.
 */
class MemoryMappedFileDriver(
    val file: File
) : RandomFileAccessDriver {

    private val randomAccessFile: RandomAccessFile
    private var closed = false

    init {
        require(this.file.length() < Int.MAX_VALUE) { "File '${file.absolutePath}' is too large to be used with the MemoryMappedFileDriver!" }
        this.randomAccessFile = RandomAccessFile(this.file, "r")
    }

    override val fileSize: Long by lazy {
        // we assume that the file size doesn't change
        // while we're reading, so we can cache it.
        this.randomAccessFile.length()
    }

    override val filePath: String
        get() = this.file.absolutePath

    override fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes? {
        require(offset >= 0) { "Argument 'offset' must not be negative, but got: ${offset}!" }
        require(bytesToRead >= 0) { "Argument 'bytesToRead' must not be negative, but got: ${offset}!" }
        check(!this.closed) { "This file access driver on '${this.file.absolutePath}' has already been closed!" }
        if (offset > this.fileSize) {
            // offset is outside the file!
            return null
        }
        // since the "read" method only supports integer offsets,
        // we have to perform a "seek" first. This will store an
        // implicit offset in the RandomAccessFile, and the integer
        // offset specified in "read" will be relative to this position.
        this.randomAccessFile.seek(offset)

        val buffer = ByteArray(bytesToRead)
        var n = 0
        do {
            val count = this.randomAccessFile.read(buffer, n, bytesToRead - n)
            if (count < 0) {
                // end of file reached
                return null
            }
            n += count
        } while (n < bytesToRead)
        return Bytes(buffer)
    }

    override fun copy(): MemoryMappedFileDriver {
        return MemoryMappedFileDriver(this.file)
    }

    override fun toString(): String {
        return "MemoryMappedFileDriver[${this.file.path}]"
    }

    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.randomAccessFile.close()
    }


    object Factory : RandomFileAccessDriverFactory {

        override fun createDriver(file: VirtualFile): RandomFileAccessDriver {
            return when (file) {
                is DiskBasedVirtualFile -> MemoryMappedFileDriver(file.fileOnDisk)
                // fall back to in-memory driver
                is InMemoryVirtualFile -> InMemoryFileDriver(file)
                else -> throw IllegalArgumentException("Unknown subclass of ${VirtualFile::class.simpleName}: ${file::class.qualifiedName}")
            }
        }

    }

}