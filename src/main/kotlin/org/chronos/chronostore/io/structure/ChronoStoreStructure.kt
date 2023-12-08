package org.chronos.chronostore.io.structure

object ChronoStoreStructure {

    // directory names

    val WRITE_AHEAD_LOG_DIR_NAME = "writeAheadLog"

    val CHECKPOINT_DIR_NAME = "checkpoints"

    // file name parts

    val WRITE_AHEAD_LOG_FILE_PREFIX = "wal"

    val WRITE_AHEAD_LOG_FILE_SUFFIX = ".log"

    val STORE_INFO_FILE_NAME = "storeInfo.json"

    val CHECKPOINT_FILE_EXTENSION = ".json"

    val CHECKPOINT_FILE_PREFIX = "checkpoint-"

}