package org.chronos.chronostore.test.util

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.impl.ChronoStoreImpl
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFileSystem
import java.nio.file.Files

enum class ChronoStoreMode {

    INMEMORY {

        override fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStoreImpl, VirtualFileSystem) -> T): T {
            val vfs = InMemoryVirtualFileSystem()
            val chronoStore = ChronoStore.openOnVirtualFileSystem(vfs, config) as ChronoStoreImpl
            try {
                return action(chronoStore, vfs)
            } finally {
                chronoStore.close()
            }
        }

    },

    ONDISK {

        override fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStoreImpl, VirtualFileSystem) -> T): T {
            val testDir = Files.createTempDirectory("chronostoreTest").toFile()
            try {
                val vfs = DiskBasedVirtualFileSystem(testDir, config.createVirtualFileSystemConfiguration())
                val chronoStore = ChronoStore.openOnVirtualFileSystem(vfs, config) as ChronoStoreImpl
                return chronoStore.use { store ->
                    action(store, vfs)
                }
            } finally {
                testDir.deleteRecursively()
            }
        }

    },
    ;

    fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStoreImpl) -> T): T {
        return withChronoStore(config) { store, _ -> action(store) }
    }

    abstract fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStoreImpl, VirtualFileSystem) -> T): T

    fun <T> withChronoStore(action: (ChronoStoreImpl) -> T): T {
        return this.withChronoStore(ChronoStoreConfiguration(), action)
    }

    fun <T> withChronoStoreAndVFS(action: (ChronoStoreImpl, VirtualFileSystem) -> T): T {
        return this.withChronoStore(ChronoStoreConfiguration(), action)
    }

}