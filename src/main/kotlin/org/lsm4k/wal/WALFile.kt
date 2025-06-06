package org.lsm4k.wal

import com.google.common.hash.Hashing
import org.lsm4k.io.vfs.VirtualReadWriteFile
import org.lsm4k.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.lsm4k.util.IOExtensions.withInputStream
import org.lsm4k.util.LittleEndianExtensions.readLittleEndianInt
import org.lsm4k.util.LittleEndianExtensions.writeLittleEndianInt
import org.lsm4k.util.io.ChecksumUtils.computeHash
import java.io.InputStream
import java.io.OutputStream

class WALFile(
    val file: VirtualReadWriteFile,
    val sequenceNumber: Long,
) {

    companion object {

        /**
         * The hash function to use for WAL files.
         *
         * Do not change this value, or all WAL files out there will become permanently invalid!
         */
        private val HASH_FUNCTION = Hashing.murmur3_32_fixed()

    }

    val checksumFile: VirtualReadWriteFile = this.file.parent?.file(this.file.name + ".murmur3")
        ?: throw IllegalStateException("Could not determine parent directory of Write-Ahead-Log file '${file.path}'!")

    init {
        require(sequenceNumber >= 0) { "Argument 'sequenceNumber' (${sequenceNumber}) must not be negative!" }
    }

    fun isFull(maxWalFileSizeBytes: Long): Boolean {
        return this.length >= maxWalFileSizeBytes
    }

    val length: Long
        get() = this.file.length

    fun <T> append(action: (OutputStream) -> T): T {
        return this.file.append(action)
    }

    fun inputStream(): InputStream {
        return this.file.createInputStream()
    }

    inline fun <T> withInputStream(action: (InputStream) -> T): T {
        return this.file.withInputStream(action)
    }


    fun delete() {
        if (this.checksumFile.exists()) {
            this.checksumFile.delete()
        }
        return this.file.delete()
    }

    fun createChecksumFileIfNecessary() {
        if (checksumFile.exists()) {
            // we already have a checksum for this file. Skip.
            return
        }
        // compute the actual checksum and write it into the target file.
        val checksum = this.file.computeHash(HASH_FUNCTION).asInt()
        this.checksumFile.deleteOverWriterFileIfExists()
        this.checksumFile.withOverWriter { overWriter ->
            overWriter.outputStream.writeLittleEndianInt(checksum)
            overWriter.commit()
        }
    }

    fun validateChecksum(): Boolean? {
        if (!this.checksumFile.exists()) {
            // we have no checksum to check against
            return null
        }
        val knownChecksum = this.checksumFile.withInputStream { inputStream ->
            inputStream.readLittleEndianInt()
        }
        val actualChecksum = this.file.computeHash(HASH_FUNCTION).asInt()
        return knownChecksum == actualChecksum
    }

    override fun toString(): String {
        return this.file.name
    }

}