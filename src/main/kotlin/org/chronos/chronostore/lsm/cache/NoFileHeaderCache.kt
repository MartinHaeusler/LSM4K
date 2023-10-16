package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.vfs.VirtualFile

data object NoFileHeaderCache: FileHeaderCache {

    override fun getFileHeader(file: VirtualFile, load: () -> FileHeader): FileHeader {
        // we don't have a cache, so the only choice is to load the file
        // header every time it gets accessed.
        return load()
    }

}