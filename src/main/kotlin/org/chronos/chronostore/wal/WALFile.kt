package org.chronos.chronostore.wal

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.util.ByteArrayExtensions.hex
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.io.ChecksumUtils.computeMD5
import java.io.InputStream
import java.io.OutputStream

class WALFile(
    val file: VirtualReadWriteFile,
    val minTimestamp: Timestamp,
) {

    val checksumFile: VirtualReadWriteFile = this.file.parent?.file(this.file.name + ".md5")
        ?: throw IllegalStateException("Could not determine parent directory of Write-Ahead-Log file '${file.path}'!")

    init {
        require(minTimestamp >= 0) { "Argument 'minTimestamp' (${minTimestamp}) must not be negative!" }
    }

    fun isFull(maxWalFileSizeBytes: Long): Boolean {
        return this.length >= maxWalFileSizeBytes
    }

    val length: Long
        get() = this.file.length

    fun <T> append(action: (OutputStream) -> T): T {
        return this.file.append(action)
    }

    fun <T> withInputStream(action: (InputStream) -> T): T {
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
        val hex = this.file.computeMD5().hex()
        this.checksumFile.deleteOverWriterFileIfExists()
        this.checksumFile.withOverWriter { overWriter ->
            overWriter.outputStream.writer().use { writer -> writer.write(hex) }
            overWriter.commit()
        }
    }

    fun validateChecksum(): Boolean? {
        if (!this.checksumFile.exists()) {
            // we have no checksum to check against
            return null
        }
        val knownChecksumHex = this.checksumFile.withInputStream { inputStream ->
            inputStream.bufferedReader().use { reader ->
                reader.readText().trim()
            }
        }
        val actualChecksumHex = this.file.computeMD5().hex()
        return knownChecksumHex == actualChecksumHex
    }

    override fun toString(): String {
        return this.file.name
    }

}