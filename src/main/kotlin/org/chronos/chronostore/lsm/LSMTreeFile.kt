package org.chronos.chronostore.lsm

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory.Companion.withDriver
import org.chronos.chronostore.io.format.ChronoStoreFileReader
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.cursor.Cursor

class LSMTreeFile(
    val virtualFile: VirtualFile,
    val index: Int,
    val driverFactory: RandomFileAccessDriverFactory,
    val blockCache: LocalBlockCache,
) {

    companion object {

        val FILE_EXTENSION = ".chronostore"

    }

    init {
        require(this.virtualFile.name.endsWith(FILE_EXTENSION)) {
            "The file '${this.virtualFile.path}' is no LSM tree file!"
        }
    }

    val header by lazy {
        this.driverFactory.withDriver(this.virtualFile, ChronoStoreFileReader::loadFileHeader)
    }

    fun get(keyAndTimestamp: KeyAndTimestamp): Command? {
        this.driverFactory.withDriver(this.virtualFile) { driver ->
            ChronoStoreFileReader(driver, this.header, this.blockCache).use { reader ->
                return reader.get(keyAndTimestamp)
            }
        }
    }

    fun cursor(): Cursor<KeyAndTimestamp, Command> {
        val driver = this.driverFactory.createDriver(this.virtualFile)
        val file = ChronoStoreFileReader(driver, this.header, this.blockCache)
        val cursor = file.openCursor()
        return cursor.onClose {
            file.close()
            driver.close()
        }
    }

}