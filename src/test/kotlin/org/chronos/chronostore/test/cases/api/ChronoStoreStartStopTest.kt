package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.junit.jupiter.api.Test
import java.nio.file.Files

class ChronoStoreStartStopTest {

    @Test
    fun canOpenAndCloseChronoStoreInstanceInMemory() {
        val store = ChronoStore.openInMemory(ChronoStoreConfiguration())
        store.close()
    }

    @Test
    fun canOpenAndCloseChronoStoreInstanceOnDisk() {
        val file = Files.createTempDirectory("chronostoreTest").toFile()
        try{
            val store = ChronoStore.openOnDirectory(file, ChronoStoreConfiguration())
            store.close()
        }finally {
            file.deleteRecursively()
        }
    }

}