package org.chronos.chronostore.io.format

import org.chronos.chronostore.api.exceptions.BlockReadException
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import java.util.concurrent.CompletableFuture

interface BlockLoader {

    companion object {

        fun basic(
            driverFactory: RandomFileAccessDriverFactory,
            headerCache: FileHeaderCache = FileHeaderCache.NONE,
        ): BlockLoader {
            return BasicBlockLoader(driverFactory, headerCache)
        }

    }

    val isAsyncSupported: Boolean

    fun getBlockOrNull(file: VirtualFile, blockIndex: Int): DataBlock? {
        return this.getBlockAsync(file, blockIndex).get()
    }

    fun getBlock(file: VirtualFile, blockIndex: Int): DataBlock {
        return this.getBlockOrNull(file, blockIndex)
            ?: throw BlockReadException("Could not read block #${blockIndex} from file '${file.path}' because the file does not have that many blocks!")
    }

    fun getBlockAsync(file: VirtualFile, blockIndex: Int): CompletableFuture<DataBlock?>

}