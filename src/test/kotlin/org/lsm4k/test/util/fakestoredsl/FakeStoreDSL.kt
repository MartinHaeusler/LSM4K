package org.lsm4k.test.util.fakestoredsl

import org.lsm4k.api.compaction.LeveledCompactionStrategy
import org.lsm4k.api.compaction.TieredCompactionStrategy
import org.lsm4k.io.vfs.VirtualFileSystem
import org.lsm4k.util.StoreId

@DslMarker
annotation class FakeStoreDSL {

    companion object {

        @FakeStoreDSL
        fun VirtualFileSystem.createFakeLeveledStore(storeName: String = "test", configure: FakeStoreConfigurator.() -> Unit): FakeCompactableStore {
            val storeId = StoreId.of(storeName)
            val storeDir = this.mkdirs(storeId.path)
            val configurator = FakeStoreConfiguratorImpl(storeId, storeDir)
            configurator.compactionStrategy = LeveledCompactionStrategy()
            configure(configurator)
            return configurator.build()
        }

        @FakeStoreDSL
        fun VirtualFileSystem.createFakeTieredStore(storeName: String = "test", configure: FakeStoreConfigurator.() -> Unit): FakeCompactableStore {
            val storeId = StoreId.of(storeName)
            val storeDir = this.mkdirs(storeId.path)
            val configurator = FakeStoreConfiguratorImpl(storeId, storeDir)
            configurator.compactionStrategy = TieredCompactionStrategy()
            configure(configurator)
            return configurator.build()
        }

    }

}

