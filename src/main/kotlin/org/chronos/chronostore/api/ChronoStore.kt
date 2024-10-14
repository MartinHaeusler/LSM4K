package org.chronos.chronostore.api

import org.chronos.chronostore.impl.ChronoStoreImpl
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFileSystem
import java.io.File

interface ChronoStore : AutoCloseable {

    companion object {

        @JvmStatic
        @JvmOverloads
        fun openOnDirectory(directory: File, configuration: ChronoStoreConfiguration = ChronoStoreConfiguration()): ChronoStore {
            require(directory.exists() && directory.isDirectory) {
                "Argument 'directory' either doesn't exist or is not a directory: ${directory.absolutePath}"
            }
            val vfsConfig = configuration.createVirtualFileSystemConfiguration()
            val vfs = DiskBasedVirtualFileSystem(directory, vfsConfig)
            return openOnVirtualFileSystem(vfs, configuration)
        }

        @JvmStatic
        @JvmOverloads
        fun openInMemory(configuration: ChronoStoreConfiguration = ChronoStoreConfiguration()): ChronoStore {
            val vfs = InMemoryVirtualFileSystem()
            return openOnVirtualFileSystem(vfs, configuration)
        }

        @JvmStatic
        fun openOnVirtualFileSystem(vfs: VirtualFileSystem, configuration: ChronoStoreConfiguration): ChronoStore {
            return ChronoStoreImpl(vfs, configuration)
        }

    }

    fun beginTransaction(): ChronoStoreTransaction

    fun <T> transaction(action: (ChronoStoreTransaction) -> T): T {
        return this.beginTransaction().use(action)
    }

    val rootPath: String

}