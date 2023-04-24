package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.io.vfs.VirtualFile

sealed interface RandomFileAccessDriverFactory {

    companion object {

        inline fun <T> RandomFileAccessDriverFactory.withDriver(file: VirtualFile, action: (RandomFileAccessDriver) -> T): T {
            return this.createDriver(file).use(action)
        }

    }

    fun createDriver(file: VirtualFile): RandomFileAccessDriver

}