package org.chronos.chronostore.api

import org.chronos.chronostore.impl.ChronoStoreImpl
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFileSystem
import java.io.File

interface ChronoStore : AutoCloseable {

    companion object {

        @JvmStatic
        fun openOnDirectory(directory: File, configuration: ChronoStoreConfiguration): ChronoStore {
            require(directory.exists() && directory.isDirectory) {
                "Argument 'directory' either doesn't exist or is not a directory: ${directory.absolutePath}"
            }
            val vfs = DiskBasedVirtualFileSystem(directory)
            return open(vfs, configuration)
        }

        @JvmStatic
        fun openInMemory(configuration: ChronoStoreConfiguration): ChronoStore {
            val vfs = InMemoryVirtualFileSystem()
            return open(vfs, configuration)
        }

        private fun open(vfs: VirtualFileSystem, configuration: ChronoStoreConfiguration): ChronoStore {
            return ChronoStoreImpl(vfs, configuration)
        }

    }

    fun beginTransaction(): ChronoStoreTransaction

    fun <T> transaction(action: (ChronoStoreTransaction) -> T): T {
        return this.beginTransaction().use(action)
    }

}