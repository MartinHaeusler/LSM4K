package org.chronos.chronostore.io.format

import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianIntOrNull
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.bytes.Bytes

object ChronoStoreFileFormat {

    val FILE_MAGIC_BYTES = Bytes.wrap(
        byteArrayOf(
            0b01100011, // c
            0b01101000, // h
            0b01110010, // r
            0b01101111, // o
            0b01101110, // n
            0b01101111, // o
            0b01110011, // s
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

    enum class Version(
        val versionString: String,
        val versionInt: Int,
    ) {

        V_1_0_0("1.0.0", 1_000_000) {

            override fun readFileHeader(driver: RandomFileAccessDriver): FileHeader {
                val trailer = this.readTrailer(driver)
                val metadata = this.readMetaData(driver, trailer)
                val indexOfBlocks = this.readIndexOfBlocks(driver, trailer)
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
                return metadataBytes.withInputStream(FileMetaData::readFrom)
            }

            private fun readIndexOfBlocks(driver: RandomFileAccessDriver, trailer: FileTrailer): IndexOfBlocks {
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
                    IndexOfBlocks(entries, trailer.beginOfIndexOfBlocks)
                }
            }

        }

        ;

        companion object {

            fun fromString(string: String): Version {
                val trim = string.trim()
                for (literal in Version.entries) {
                    if (literal.versionString.equals(trim, ignoreCase = true)) {
                        return literal
                    }
                }
                throw IllegalArgumentException("Unknown ChronoStore file format version: '${string}'!")
            }

            fun fromInt(int: Int): Version {
                for (literal in Version.entries) {
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

}