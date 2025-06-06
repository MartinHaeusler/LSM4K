package org.lsm4k.test.util

import org.lsm4k.io.vfs.VirtualFileSystem
import org.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystem
import org.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.lsm4k.io.vfs.inmemory.InMemoryVirtualFileSystem
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
            val dir = Files.createTempDirectory("lsm4kTest").toFile()
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