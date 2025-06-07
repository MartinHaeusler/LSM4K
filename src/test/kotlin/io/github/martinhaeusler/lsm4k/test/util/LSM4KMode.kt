package io.github.martinhaeusler.lsm4k.test.util

import io.github.martinhaeusler.lsm4k.api.DatabaseEngine
import io.github.martinhaeusler.lsm4k.api.LSM4KConfiguration
import io.github.martinhaeusler.lsm4k.impl.DatabaseEngineImpl
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystem
import io.github.martinhaeusler.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystem
import io.github.martinhaeusler.lsm4k.io.vfs.inmemory.InMemoryVirtualFileSystem
import java.nio.file.Files

enum class LSM4KMode {

    INMEMORY {

        override fun <T> withDatabaseEngine(config: LSM4KConfiguration, action: (DatabaseEngineImpl, VirtualFileSystem) -> T): T {
            val vfs = InMemoryVirtualFileSystem()
            val databaseEngine = DatabaseEngine.openOnVirtualFileSystem(vfs, config) as DatabaseEngineImpl
            return databaseEngine.use { action(it, vfs) }
        }

    },

    ONDISK {

        override fun <T> withDatabaseEngine(config: LSM4KConfiguration, action: (DatabaseEngineImpl, VirtualFileSystem) -> T): T {
            val testDir = Files.createTempDirectory("lsm4kTest").toFile()
            try {
                val vfs = DiskBasedVirtualFileSystem(testDir, config.createVirtualFileSystemConfiguration())
                val databaseEngine = DatabaseEngine.openOnVirtualFileSystem(vfs, config) as DatabaseEngineImpl
                return databaseEngine.use { store ->
                    action(store, vfs)
                }
            } finally {
                testDir.deleteRecursively()
            }
        }

    },
    ;

    fun <T> withDatabaseEngine(config: LSM4KConfiguration, action: (DatabaseEngineImpl) -> T): T {
        return withDatabaseEngine(config) { store, _ -> action(store) }
    }

    abstract fun <T> withDatabaseEngine(config: LSM4KConfiguration, action: (DatabaseEngineImpl, VirtualFileSystem) -> T): T

    fun <T> withDatabaseEngine(action: (DatabaseEngineImpl) -> T): T {
        return this.withDatabaseEngine(LSM4KConfiguration(), action)
    }

    fun <T> withDatabaseEngineAndVFS(action: (DatabaseEngineImpl, VirtualFileSystem) -> T): T {
        return this.withDatabaseEngine(LSM4KConfiguration(), action)
    }

}