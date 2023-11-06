package org.chronos.chronostore.test.util

import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFileSystem
import java.nio.file.Files

enum class VFSMode {

    INMEMORY {

        override fun <T> withVFS(action: (VirtualFileSystem) -> T): T {
            val vfs = InMemoryVirtualFileSystem()
            return action(vfs)
        }
    },

    ONDISK {

        override fun <T> withVFS(action: (VirtualFileSystem) -> T): T {
            val dir = Files.createTempDirectory("chronoStoreTest").toFile()
            try {
                val vfs = DiskBasedVirtualFileSystem(dir, DiskBasedVirtualFileSystemSettings())
                return action(vfs)
            } finally {
                dir.deleteRecursively()
            }
        }

    };

    abstract fun <T> withVFS(action: (VirtualFileSystem) -> T): T

}