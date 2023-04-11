package org.chronos.chronostore.impl

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.io.structure.ChronoStoreStructure
import org.chronos.chronostore.io.vfs.VirtualFileSystem

class ChronoStoreImpl(
    private val vfs: VirtualFileSystem,
    private val configuration: ChronoStoreConfiguration
) : ChronoStore {

    private var isOpen = true

    init {
        val walFile = this.vfs.file(ChronoStoreStructure.WAL_FILE_NAME)
        if (!walFile.exists()) {
            // The WAL file doesn't exist. It's a new, empty database.
            // We don't need a recovery, but we have to "set up camp".
            this.createNewEmptyDatabase()
        } else {
            // the WAL file exists, perform startup recovery.
            this.performStartupRecovery()
        }
    }

    private fun createNewEmptyDatabase() {

    }

    private fun performStartupRecovery() {

    }

    override val storeManager: StoreManager
        get() = TODO("Not yet implemented")

    override fun beginTransaction(): ChronoStoreTransaction {
        TODO("Not yet implemented")
    }

    override fun close() {
        if (!isOpen) {
            return
        }
        isOpen = false
    }

}