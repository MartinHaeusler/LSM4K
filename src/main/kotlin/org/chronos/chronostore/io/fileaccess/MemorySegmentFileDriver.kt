package org.chronos.chronostore.io.fileaccess

import mu.KotlinLogging
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFile
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.util.bytes.Bytes
import java.io.File
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

/**
 * A [RandomFileAccessDriver] based on the new JDK [MemorySegment] API.
 *
 * In contrast to the [MemoryMappedFileDriver], this driver can handle files
 * larger than 2GB.
 */
class MemorySegmentFileDriver(
    private val file: File,
) : RandomFileAccessDriver {

    // TODO [PERFORMANCE]: Attempt to use "ExtendedOpenOption.DIRECT". This requires aligning the memory to the file system page size.
    // See: https://stackoverflow.com/a/73625490/3094906
    private val channel: FileChannel = FileChannel.open(this.file.toPath(), StandardOpenOption.READ)
    private val arena = Arena.ofShared()
    private val memorySegment: MemorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena)
    private var closed = false

    override val fileSize: Long by lazy {
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

        return Bytes.wrap(slice.toArray(ValueLayout.JAVA_BYTE))
    }

    override fun copy(): MemorySegmentFileDriver {
        return MemorySegmentFileDriver(this.file)
    }

    override fun toString(): String {
        return "MemorySegmentFileDriver[${this.file.path}]"
    }


    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.arena.close()
    }

    object Factory : RandomFileAccessDriverFactory {

        private val log = KotlinLogging.logger { }

        val isAvailable: Boolean by lazy {
            // checks if the MemorySegment API is available in the current context.
            // It requires the JVM option "--add-modules jdk.incubator.foreign".
            try {
                MemorySegment::class.qualifiedName
                return@lazy true
            } catch (_: Throwable) {
                log.warn { "Memory Segment API is not available (JRE is too old or the feature flag is disabled). Falling back to File Channels." }
                return@lazy false
            }
        }

        override fun createDriver(file: VirtualFile): RandomFileAccessDriver {
            return when (file) {
                is DiskBasedVirtualFile -> if (isAvailable) {
                    MemorySegmentFileDriver(file.fileOnDisk)
                } else {
                    FileChannelDriver(file.fileOnDisk)
                }
                // fall back to in-memory driver
                is InMemoryVirtualFile -> InMemoryFileDriver(file)
                else -> throw IllegalArgumentException("Unknown subclass of ${VirtualFile::class.simpleName}: ${file::class.qualifiedName}")
            }
        }

    }

}