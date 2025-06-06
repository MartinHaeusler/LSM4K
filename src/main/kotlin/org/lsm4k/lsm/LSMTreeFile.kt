package org.lsm4k.lsm

import org.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import org.lsm4k.io.fileaccess.RandomFileAccessDriverFactory.Companion.withDriver
import org.lsm4k.io.format.FileHeader
import org.lsm4k.io.format.LSMFileFormat
import org.lsm4k.io.format.cursor.LSMFileCursor
import org.lsm4k.io.vfs.VirtualFile
import org.lsm4k.lsm.cache.BlockCache
import org.lsm4k.lsm.cache.FileHeaderCache
import org.lsm4k.model.command.Command
import org.lsm4k.model.command.KeyAndTSN
import org.lsm4k.util.FileIndex
import org.lsm4k.util.cursor.Cursor

class LSMTreeFile(
    val virtualFile: VirtualFile,
    val index: FileIndex,
    val driverFactory: RandomFileAccessDriverFactory,
    val blockCache: BlockCache,
    val fileHeaderCache: FileHeaderCache,
) {

    companion object {

        const val FILE_EXTENSION = ".lsm"

    }

    init {
        require(this.virtualFile.name.endsWith(FILE_EXTENSION)) {
            "The file '${this.virtualFile.path}' is no LSM tree file!"
        }
    }

    val header: FileHeader
        get() = this.fileHeaderCache.getFileHeader(this.virtualFile, this::loadFileHeaderUncached)

    private fun loadFileHeaderUncached(): FileHeader {
        return this.driverFactory.withDriver(this.virtualFile, LSMFileFormat::loadFileHeader)
    }

    /**
     * Gets the latest version of the value associated with the given [keyAndTSN] in this tree.
     *
     * @param keyAndTSN The [KeyAndTSN] to search for.
     *
     * @return The latest visible version (maximum commit TSN <= given TSN) for the given [keyAndTSN], or `null` if none was found.
     */
    fun getLatestVersion(keyAndTSN: KeyAndTSN): Command? {
        return LSMFileFormat.getLatestVersion(this.virtualFile, this.header, keyAndTSN, this.blockCache)
    }

    fun cursor(): Cursor<KeyAndTSN, Command> {
        return LSMFileCursor(
            file = this.virtualFile,
            fileHeader = this.header,
            blockLoader = this.blockCache,
        )
    }


    override fun toString(): String {
        return "LSMTreeFile[${this.virtualFile}]"
    }
}