package org.chronos.chronostore.test.util

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.impl.ChronoStoreImpl
import java.nio.file.Files

enum class ChronoStoreMode {

    INMEMORY {

        override fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStoreImpl) -> T): T {
            val chronoStore = ChronoStore.openInMemory(config) as ChronoStoreImpl
            return chronoStore.use(action)
        }

    },

    ONDISK {

        override fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStoreImpl) -> T): T {
            val testDir = Files.createTempDirectory("chronostoreTest").toFile()
            try {
                val chronoStore = ChronoStore.openOnDirectory(testDir, config) as ChronoStoreImpl
                return chronoStore.use(action)
            } finally {
                testDir.deleteRecursively()
            }
        }

    },
    ;

    abstract fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStoreImpl) -> T): T

    fun <T> withChronoStore(action: (ChronoStoreImpl) -> T): T {
        return this.withChronoStore(ChronoStoreConfiguration(), action)
    }


}