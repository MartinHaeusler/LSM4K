package org.lsm4k.io.format

class FileHeader(
    val fileFormatVersion: FileFormatVersion,
    val trailer: FileTrailer,
    val metaData: FileMetaData,
    val indexOfBlocks: IndexOfBlocks,
) {

    val sizeBytes: Long
        get(){
            // enums are internally stored as integers
            val formatVersionSize = Int.SIZE_BYTES
            val trailerSize = trailer.sizeBytes
            val metadataSize = metaData.sizeBytes
            val indexOfBlocksSize = indexOfBlocks.sizeInBytes
            return formatVersionSize + trailerSize + metadataSize + indexOfBlocksSize
        }

}