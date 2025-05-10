package org.chronos.chronostore.api

import org.chronos.chronostore.api.ChronoStore.Companion.openInMemory
import org.chronos.chronostore.api.ChronoStore.Companion.openOnDirectory
import org.chronos.chronostore.impl.ChronoStoreImpl
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFileSystem
import org.chronos.chronostore.util.report.ChronoStoreReport
import java.io.File

/**
 * A single [ChronoStore] instance.
 *
 * This is the main top-level interface of the API.
 *
 * Capable of creating stores and transactions on them.
 *
 * To create an instance of this class, please use one of the static `open...` methods
 * (e.g. [openOnDirectory] or [openInMemory]).
 *
 * **Attention:** This interface extends [AutoCloseable]. Instances must therefore
 * be closed **explicitly** by calling the [close] method!
 *
 * **Attention:** Closing a [ChronoStore] instance will also terminate all open
 * transactions!
 *
 * **Note:** Usually, every program will only need one instance as it can support
 * several [Store]s simultaneously, but there is no inherent limitation
 * which would enforce this.
 *
 */
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
        fun openOnVirtualFileSystem(vfs: VirtualFileSystem, configuration: ChronoStoreConfiguration = ChronoStoreConfiguration()): ChronoStore {
            return ChronoStoreImpl(vfs, configuration)
        }

    }

    // TODO [FEATURE]: We should support and distinguish between read-only transactions, read-write transactions, and exclusive transactions.
    //   Read-Only: commit throws an exception. Optionally allow to configure if transient changes are allowed or not.
    //   Read-Write: allows commits, but is not exclusive.
    //   Exclusive: allows commits and blocks all concurrent read-write and exclusive transactions from being executed while it's active (read-only transactions may still execute)
    fun beginTransaction(): ChronoStoreTransaction

    fun <T> transaction(action: (ChronoStoreTransaction) -> T): T {
        return this.beginTransaction().use(action)
    }

    val rootPath: String

    /** Creates a read-only snapshot report about the structure of the store. */
    fun statusReport(): ChronoStoreReport

}