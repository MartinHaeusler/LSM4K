package org.lsm4k.io.format

import org.lsm4k.api.exceptions.BlockReadException
import org.lsm4k.io.fileaccess.RandomFileAccessDriver
import org.lsm4k.io.format.datablock.DataBlock
import org.lsm4k.io.vfs.VirtualFile
import org.lsm4k.model.command.Command
import org.lsm4k.model.command.KeyAndTSN
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.statistics.StatisticsReporter

object LSMFileFormat {

    val FILE_MAGIC_BYTES = Bytes.wrap(
        byteArrayOf(
            0b01101100, // l
            0b01110011, // s
            0b01101101, // m
            0b00110100, // 4
            0b01101011, // k
            0b00000000, // <null>
            0b00000000, // <null>
            0b00000000, // <null>
        )
    )

    val BLOCK_MAGIC_BYTES = Bytes.wrap(
        byteArrayOf(
            0b01100010, // b
            0b01101100, // l
            0b01101111, // o
            0b01100011, // c
            0b01101011, // k
            0b01100010, // b
            0b01100111, // g
            0b01101110, // n
        )               // blockbgn = block begin
    )

    @JvmStatic
    fun loadFileHeader(driver: RandomFileAccessDriver): FileHeader {
        // read and validate the magic bytes
        val magicBytesAndVersion = driver.readBytes(0, FILE_MAGIC_BYTES.size + Int.SIZE_BYTES)
        val magicBytes = magicBytesAndVersion.slice(0, FILE_MAGIC_BYTES.size)

        if (magicBytes != FILE_MAGIC_BYTES) {
            throw IllegalStateException("The file '${driver.filePath}' has an unknown file format. Expected ${FILE_MAGIC_BYTES.hex()} but got ${magicBytes.hex()}!")
        }
        val versionInt = magicBytesAndVersion.readLittleEndianInt(FILE_MAGIC_BYTES.size)
        val fileFormatVersion = FileFormatVersion.fromInt(versionInt)
        return fileFormatVersion.readFileHeader(driver)
    }

    @JvmStatic
    fun loadBlockFromFileOrNull(driver: RandomFileAccessDriver, fileHeader: FileHeader, blockIndex: Int, statisticsReporter: StatisticsReporter): DataBlock? {
        val timeBefore = System.currentTimeMillis()
        val (startPosition, length) = fileHeader.indexOfBlocks.getBlockStartPositionAndLengthOrNull(blockIndex)
            ?: return null

        return loadBlockInternal(
            driver = driver,
            startPosition = startPosition,
            length = length,
            fileHeader = fileHeader,
            timeBefore = timeBefore,
            blockIndex = blockIndex,
            statisticsReporter = statisticsReporter,
        )
    }

    @JvmStatic
    fun loadBlockFromFile(driver: RandomFileAccessDriver, fileHeader: FileHeader, blockIndex: Int, statisticsReporter: StatisticsReporter): DataBlock {
        val timeBefore = System.currentTimeMillis()
        val (startPosition, length) = fileHeader.indexOfBlocks.getBlockStartPositionAndLengthOrNull(blockIndex)
            ?: throw IllegalStateException(
                "Could not fetch block #${blockIndex} in file: '${driver.filePath}')!" +
                    " The block index contains ${fileHeader.indexOfBlocks.size} entries."
            )
        return loadBlockInternal(
            driver = driver,
            startPosition = startPosition,
            length = length,
            fileHeader = fileHeader,
            timeBefore = timeBefore,
            blockIndex = blockIndex,
            statisticsReporter = statisticsReporter,
        )
    }

    private fun loadBlockInternal(
        driver: RandomFileAccessDriver,
        startPosition: Long,
        length: Int,
        fileHeader: FileHeader,
        timeBefore: Long,
        blockIndex: Int,
        statisticsReporter: StatisticsReporter,
    ): DataBlock {
        val blockBytes = driver.readBytes(startPosition, length)
        val compressionAlgorithm = fileHeader.metaData.settings.compression
        try {
            val dataBlock = DataBlock.loadBlock(
                input = blockBytes,
                compressionAlgorithm = compressionAlgorithm,
                statisticsReporter = statisticsReporter,
            )
            val timeAfter = System.currentTimeMillis()
            statisticsReporter.reportBlockLoadTime(timeAfter - timeBefore)
            return dataBlock
        } catch (e: Exception) {
            throw BlockReadException(
                message = "Failed to read block #${blockIndex} of file '${driver.filePath}'." +
                    " This file is potentially corrupted! Cause: ${e}",
                cause = e
            )
        }
    }

    @JvmStatic
    fun getLatestVersion(file: VirtualFile, fileHeader: FileHeader, keyAndTSN: KeyAndTSN, blockLoader: BlockLoader): Command? {
        if (!fileHeader.metaData.mayContainKey(keyAndTSN.key)) {
            // key is definitely not in this file, no point in searching.
            return null
        }
        if (!fileHeader.metaData.mayContainDataRelevantForTSN(keyAndTSN.tsn)) {
            // the data in this file is too new and doesn't contain anything relevant for the request timestamp.
            return null
        }
        // the key may be contained, let's check.
        var blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampDescending(keyAndTSN)
            ?: return null // we don't have a block for this key and timestamp.
        var matchingCommandFromPreviousBlock: Command? = null
        while (true) {
            val dataBlock = blockLoader.getBlockOrNull(file, blockIndex)
                ?: return matchingCommandFromPreviousBlock
            val (command, isLastInBlock) = dataBlock.get(keyAndTSN)
                ?: return matchingCommandFromPreviousBlock
            if (!isLastInBlock) {
                return command
            }
            // we've hit the last key in the block, so we need to consult the next block
            // in order to see if we find a newer entry which matches the key-and-timestamp.
            matchingCommandFromPreviousBlock = command
            // check the next block
            blockIndex++
        }
    }

}