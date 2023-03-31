package org.chronos.chronostore.io.format

import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver

class ChronoStoreFileReader : AutoCloseable {

    private val driver: RandomFileAccessDriver

    private val fileMetaData: FileMetaData

    constructor(driver: RandomFileAccessDriver) {
        this.driver = driver
        this.fileMetaData = this.loadFileMetaData()
    }

    /**
     * Internal constructor for copying a reader.
     *
     * The initialization phase of a ChronoStore file comes with some
     * overhead, which is skipped here because the file is immutable
     * and the reader we were copied from already did all the work for us.
     */
    private constructor(driver: RandomFileAccessDriver, fileMetaData: FileMetaData) {
        // skip all the validation, it has already been done for us.
        this.driver = driver
        this.fileMetaData = fileMetaData
    }

    private fun loadFileMetaData(): FileMetaData {
        // read and validate the magic bytes
        val bytes = driver.readBytes(0, 8)
        if (bytes != ChronoStoreFileFormat.FILE_MAGIC_BYTES) {
            throw IllegalStateException("The file '${driver.filePath}' has an unknown file format.")
        }
        TODO("implement me!")
    }

    fun get(keyAndTimestamp: KeyAndTimestamp): Command {
        TODO("Implement me!")
    }

    fun scan(from: KeyAndTimestamp?, fromInclusive: Boolean, to: KeyAndTimestamp?, toInclusive: Boolean, consumer: ScanClient) {
        return if (to == null) {
            scan(from, fromInclusive, consumer)
        } else {
            scan(from, fromInclusive, ScanUntilUpperLimitClient(consumer, to, toInclusive))
        }
    }

    fun scan(from: KeyAndTimestamp?, fromInclusive: Boolean, consumer: ScanClient) {
        TODO("Implement me!")
    }

    // TODO: maybe cursor API instead of just scans?

    /**
     * Creates a copy of this reader.
     *
     * Readers do not allow concurrent access because the drivers
     * are stateful (e.g. "seek" positions, OS constraints, etc).
     * In order to still allow multiple reader threads to access
     * the same file, readers can be copied.
     *
     * The copied reader will have no relationship with `this`
     * reader (except that both read the same file). It will need
     * to be managed and [closed][close] separately.
     *
     * @return A copy of this reader, for use by another thread.
     */
    fun copy(): ChronoStoreFileReader {
        return ChronoStoreFileReader(this.driver.copy(), this.fileMetaData)
    }

    override fun close() {
        this.driver.close()
    }

    enum class ScanControl {

        CONTINUE,

        STOP;

    }

    fun interface ScanClient {

        fun inspect(command: Command): ScanControl

    }

    private class ScanUntilUpperLimitClient(
        private val consumer: ScanClient,
        private val upperBound: KeyAndTimestamp,
        private val upperBoundInclusive: Boolean,
    ) : ScanClient {

        override fun inspect(command: Command): ScanControl {
            val cmp = command.keyAndTimestamp.compareTo(this.upperBound)
            return when {
                cmp < 0 -> {
                    // upper bound hasn't been reached, keep going
                    this.consumer.inspect(command)
                }

                cmp > 0 -> {
                    // upper bound has been exceeded
                    ScanControl.STOP
                }

                else -> {
                    // we're AT the upper bound, now it depends if we want to see it
                    if (upperBoundInclusive) {
                        this.consumer.inspect(command)
                    } else {
                        ScanControl.STOP
                    }
                }
            }
        }

    }

}