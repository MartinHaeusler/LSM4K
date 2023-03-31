package org.chronos.chronostore.io.fileaccess

import jdk.incubator.foreign.MemorySegment
import jdk.incubator.foreign.ResourceScope
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFile
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.util.Bytes
import java.io.File
import java.nio.channels.FileChannel

/**
 * A [RandomFileAccessDriver] based on the new JDK [MemorySegment] API.
 *
 * In contrast to the [MemoryMappedFileDriver], this driver can handle files
 * larger than 2GB.
 */
class MemorySegmentFileDriver(
    private val file: File
) : RandomFileAccessDriver {

    var closed = false
    val scope: ResourceScope = ResourceScope.newConfinedScope()
    val memorySegment: MemorySegment = MemorySegment.mapFile(this.file.toPath(), 0, file.length(), FileChannel.MapMode.READ_ONLY, scope)

    override val size: Long by lazy {
        this.file.length()
    }

    override val filePath: String
        get() = this.file.absolutePath

    override fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes? {
        check(!this.closed) { "This file access driver on '${file.absolutePath}' has already been closed!" }
        if (this.memorySegment.byteSize() < offset + bytesToRead) {
            // there are not enough bytes in the segment to fulfill the request
            return null
        }
        val slice = this.memorySegment.asSlice(offset, bytesToRead.toLong())
        if (slice.byteSize() < bytesToRead) {
            // there are not enough bytes in the segment to fulfill the request
            return null
        }
        return Bytes(slice.toByteArray())
    }

    override fun copy(): MemorySegmentFileDriver {
        return MemorySegmentFileDriver(this.file)
    }

    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.scope.close()
    }

    class Factory : RandomFileAccessDriverFactory {

        companion object {

            val isAvailable: Boolean by lazy {
                // checks if the MemorySegment API is available in the current context.
                // It requires the JVM option "--add-modules jdk.incubator.foreign".
                try {
                    MemorySegment::class.qualifiedName
                    return@lazy true
                } catch (e: Throwable) {
                    return@lazy false
                }
            }

        }

        override fun createDriver(file: VirtualFile): RandomFileAccessDriver {
            return when (file) {
                is DiskBasedVirtualFile -> MemorySegmentFileDriver(file.fileOnDisk)
                // fall back to in-memory driver
                is InMemoryVirtualFile -> InMemoryFileDriver(file)
                else -> throw IllegalArgumentException("Unknown subclass of ${VirtualFile::class.simpleName}: ${file::class.qualifiedName}")
            }
        }

    }

}