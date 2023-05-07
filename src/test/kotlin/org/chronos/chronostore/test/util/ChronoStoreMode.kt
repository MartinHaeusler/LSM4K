package org.chronos.chronostore.test.util

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import java.nio.file.Files

enum class ChronoStoreMode {

    INMEMORY {

        override fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStore) -> T): T {
            return ChronoStore.openInMemory(config).use(action)
        }

    },

    ONDISK {

        override fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStore) -> T): T {
            val testDir = Files.createTempDirectory("chronostoreTest").toFile()
            try {
                return ChronoStore.openOnDirectory(testDir, config).use(action)
            } finally {
                testDir.deleteRecursively()
            }
        }

    },
    ;

    abstract fun <T> withChronoStore(config: ChronoStoreConfiguration, action: (ChronoStore) -> T): T

    fun <T> withChronoStore(action: (ChronoStore) -> T): T {
        return this.withChronoStore(ChronoStoreConfiguration(), action)
    }


}