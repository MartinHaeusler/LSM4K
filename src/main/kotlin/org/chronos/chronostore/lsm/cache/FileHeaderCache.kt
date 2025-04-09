package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.unit.BinarySize

sealed interface FileHeaderCache {

    fun getFileHeader(file: VirtualFile, load: () -> FileHeader): FileHeader

    companion object {

        val NONE: FileHeaderCache = NoFileHeaderCache

        fun create(fileHeaderCacheSize: BinarySize?): FileHeaderCache {
            return if (fileHeaderCacheSize != null) {
                FileHeaderCacheImpl(fileHeaderCacheSize)
            } else {
                NoFileHeaderCache
            }
        }

    }

}