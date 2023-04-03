package org.chronos.chronostore.io.format

import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.IOExtensions.withInputStream

object ChronoStoreFileFormat {

    val FILE_MAGIC_BYTES = Bytes(
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

    val BLOCK_MAGIC_BYTES = Bytes(
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

        V_1_0_0("1.0.0", 100_000_000) {

            override fun readTrailer(driver: RandomFileAccessDriver): FileTrailer {
                val fileLength = driver.size
                val trailerSizeInBytes = FileTrailer.SIZE_BYTES
                val trailerBytes = driver.readBytes(fileLength - trailerSizeInBytes, trailerSizeInBytes)
                return FileTrailer.readFrom(trailerBytes)
            }

            override fun readMetaData(driver: RandomFileAccessDriver, fileTrailer: FileTrailer): FileMetaData {
                val trailer = this.readTrailer(driver)
                val metadataBytes = driver.readBytes(trailer.beginOfMetadata, (driver.size - FileTrailer.SIZE_BYTES - trailer.beginOfMetadata).toInt())
                return metadataBytes.withInputStream(FileMetaData::readFrom)
            }


        }

        ;

        companion object {

            fun fromString(string: String): Version {
                val trim = string.trim()
                for (literal in values()) {
                    if (literal.versionString.equals(string, ignoreCase = true)) {
                        return literal
                    }
                }
                throw IllegalArgumentException("Unknown ChronoStore file format version: '${string}'!")
            }

            fun fromInt(int: Int): Version {
                for (literal in values()) {
                    if (literal.versionInt == int) {
                        return literal
                    }
                }
                throw IllegalArgumentException("Unknown ChronoStore file format version: '${int}'!")
            }

        }

        abstract fun readTrailer(driver: RandomFileAccessDriver): FileTrailer

        abstract fun readMetaData(driver: RandomFileAccessDriver, trailer: FileTrailer): FileMetaData

        override fun toString(): String {
            return this.versionString
        }

    }

}