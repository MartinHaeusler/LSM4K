package org.chronos.chronostore.io.format

class FileHeader(
    val fileFormatVersion: ChronoStoreFileFormat.Version,
    val trailer: FileTrailer,
    val metaData: FileMetaData,
    val indexOfBlocks: IndexOfBlocks,
)