package io.github.martinhaeusler.lsm4k.io.structure

object LSM4KStructure {

    // directory names

    const val WRITE_AHEAD_LOG_DIR_NAME = "writeAheadLog"

    // file name parts

    const val WRITE_AHEAD_LOG_FILE_PREFIX = "wal"

    const val WRITE_AHEAD_LOG_FILE_SUFFIX = ".log"

    const val LOCK_FILE_NAME = "processLock.lck"

}