package io.github.martinhaeusler.lsm4k.wal

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.util.IOExtensions.withInputStream
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.readLittleEndianLongOrNull
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A [WALSummaryFile] is a file which helps in determining if all WAL files are present.
 *
 * It contains a single [Long] (little endian encoding) which can be retrieved via
 * [getHighestDroppedWalSequenceNumber] and assigned via [setHighestDroppedWalSequenceNumber].
 * This number refers to the latest WAL file which has been deleted due to WAL shortening.
 *
 * If this file does not exist or is empty, it implies that no WAL shortening has ever
 * happened, which in turn means that the first WAL file must start with sequence number 0.
 */
class WALSummaryFile(
    val file: VirtualReadWriteFile,
) {

    private val lock = ReentrantReadWriteLock(true)

    private var highestDroppedWalSequenceNumber: Long? = loadFile()

    fun getHighestDroppedWalSequenceNumber(): Long? {
        this.lock.read {
            return this.highestDroppedWalSequenceNumber
        }
    }

    fun setHighestDroppedWalSequenceNumber(highestDroppedWalSequenceNumber: Long) {
        require(highestDroppedWalSequenceNumber >= 0) {
            "Argument 'highestDroppedWalSequenceNumber' must not be negative!"
        }
        val highestSoFar = this.highestDroppedWalSequenceNumber ?: -1
        if (highestSoFar > highestDroppedWalSequenceNumber) {
            throw IllegalStateException(
                "WAL Summary Error: highest dropped WAL file sequence number so far is ${this.highestDroppedWalSequenceNumber}" +
                    " which is greater than the new number (${highestDroppedWalSequenceNumber})." +
                    " These numbers have to be in ascending order!"
            )
        }
        if (highestSoFar == highestDroppedWalSequenceNumber) {
            return
        }
        this.lock.write {
            this.file.createOverWriter().use { overWriter ->
                overWriter.outputStream.writeLittleEndianLong(highestDroppedWalSequenceNumber)
                overWriter.commit()
            }
            this.highestDroppedWalSequenceNumber = highestDroppedWalSequenceNumber
        }
    }


    private fun loadFile(): Long? {
        if (!this.file.exists()) {
            return null
        }
        return this.file.withInputStream { input -> input.buffered().readLittleEndianLongOrNull() }
    }

}