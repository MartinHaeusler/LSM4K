package org.chronos.chronostore.io.format

import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianIntOrNull
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong

enum class FileFormatVersion(
    val versionString: String,
    val versionInt: Int,
) {

    V_1_0_0("1.0.0", 1_000_000) {

        override fun readFileHeader(driver: RandomFileAccessDriver): FileHeader {
            val trailer = this.readTrailer(driver)
            val metadata = this.readMetaData(driver, trailer)
            val indexOfBlocks = this.readIndexOfBlocks(driver, trailer, metadata.firstKeyAndTSN, metadata.lastKeyAndTSN)
            return FileHeader(
                fileFormatVersion = this,
                trailer = trailer,
                metaData = metadata,
                indexOfBlocks = indexOfBlocks,
            )
        }

        private fun readTrailer(driver: RandomFileAccessDriver): FileTrailer {
            val fileLength = driver.fileSize
            val trailerSizeInBytes = FileTrailer.SIZE_BYTES
            val trailerBytes = driver.readBytes(fileLength - trailerSizeInBytes, trailerSizeInBytes)
            return FileTrailer.readFrom(trailerBytes)
        }

        private fun readMetaData(driver: RandomFileAccessDriver, fileTrailer: FileTrailer): FileMetaData {
            val lengthOfMetadata = (driver.fileSize - FileTrailer.SIZE_BYTES - fileTrailer.beginOfMetadata).toInt()
            val metadataBytes = driver.readBytes(fileTrailer.beginOfMetadata, lengthOfMetadata)
            return metadataBytes.withInputStream(FileMetaData.Companion::readFrom)
        }

        private fun readIndexOfBlocks(driver: RandomFileAccessDriver, trailer: FileTrailer, firstKeyAndTSN: KeyAndTSN?, lastKeyAndTSN: KeyAndTSN?): IndexOfBlocks {
            val lengthOfIndexOfBlocks = (trailer.beginOfMetadata - trailer.beginOfIndexOfBlocks).toInt()
            val indexBytes = driver.readBytes(trailer.beginOfIndexOfBlocks, lengthOfIndexOfBlocks)
            return indexBytes.withInputStream { inputStream ->
                val entries = generateSequence {
                    val blockSequenceNumber = inputStream.readLittleEndianIntOrNull()
                        ?: return@generateSequence null // we're done
                    val blockStartPosition = inputStream.readLittleEndianLong()
                    val blockMinKeyAndTSN = KeyAndTSN.readFromStream(inputStream)
                    Triple(blockSequenceNumber, blockStartPosition, blockMinKeyAndTSN)
                }.toList()
                IndexOfBlocks(entries, trailer.beginOfIndexOfBlocks, firstKeyAndTSN, lastKeyAndTSN)
            }
        }

    }

    ;

    companion object {

        fun fromString(string: String): FileFormatVersion {
            val trim = string.trim()
            for (literal in FileFormatVersion.entries) {
                if (literal.versionString.equals(trim, ignoreCase = true)) {
                    return literal
                }
            }
            throw IllegalArgumentException("Unknown ChronoStore file format version: '${string}'!")
        }

        fun fromInt(int: Int): FileFormatVersion {
            for (literal in FileFormatVersion.entries) {
                if (literal.versionInt == int) {
                    return literal
                }
            }
            throw IllegalArgumentException("Unknown ChronoStore file format version: '${int}'!")
        }

    }

    abstract fun readFileHeader(driver: RandomFileAccessDriver): FileHeader


    override fun toString(): String {
        return this.versionString
    }

}