package org.chronos.chronostore.io.format

class FileHeader(
    val fileFormatVersion: ChronoStoreFileFormat.Version,
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
            val infoMapSize = metaData.infoMap.entries.sumOf { it.key.size + it.value.size }
            val indexOfBlocksSize = indexOfBlocks.sizeInBytes
            return formatVersionSize + trailerSize + metadataSize + infoMapSize + indexOfBlocksSize
        }

}