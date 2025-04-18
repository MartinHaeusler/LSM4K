package org.chronos.chronostore.lsm

import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory.Companion.withDriver
import org.chronos.chronostore.io.format.ChronoStoreFileFormat
import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.format.cursor.ChronoStoreFileCursor
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.lsm.cache.BlockCache
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.cursor.Cursor

class LSMTreeFile(
    val virtualFile: VirtualFile,
    val index: FileIndex,
    val driverFactory: RandomFileAccessDriverFactory,
    val blockCache: BlockCache,
    val fileHeaderCache: FileHeaderCache,
) {

    companion object {

        val FILE_EXTENSION = ".chronostore"

    }

    init {
        require(this.virtualFile.name.endsWith(FILE_EXTENSION)) {
            "The file '${this.virtualFile.path}' is no LSM tree file!"
        }
    }

    val header: FileHeader
        get() = this.fileHeaderCache.getFileHeader(this.virtualFile, this::loadFileHeaderUncached)

    private fun loadFileHeaderUncached(): FileHeader {
        return this.driverFactory.withDriver(this.virtualFile, ChronoStoreFileFormat::loadFileHeader)
    }

    /**
     * Gets the latest version of the value associated with the given [keyAndTSN] in this tree.
     *
     * @param keyAndTSN The [KeyAndTSN] to search for.
     *
     * @return The latest visible version (maximum commit TSN <= given TSN) for the given [keyAndTSN], or `null` if none was found.
     */
    fun getLatestVersion(keyAndTSN: KeyAndTSN): Command? {
        return ChronoStoreFileFormat.getLatestVersion(this.virtualFile, this.header, keyAndTSN, this.blockCache)
    }

    fun cursor(): Cursor<KeyAndTSN, Command> {
        return ChronoStoreFileCursor(
            file = this.virtualFile,
            fileHeader = this.header,
            blockLoader = this.blockCache,
        )
    }


    override fun toString(): String {
        return "LSMTreeFile[${this.virtualFile}]"
    }
}