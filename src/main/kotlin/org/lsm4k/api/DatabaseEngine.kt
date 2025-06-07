package org.lsm4k.api

import org.lsm4k.api.DatabaseEngine.Companion.openInMemory
import org.lsm4k.api.DatabaseEngine.Companion.openOnDirectory
import org.lsm4k.api.exceptions.TransactionLockAcquisitionException
import org.lsm4k.api.statistics.StatisticsManager
import org.lsm4k.impl.DatabaseEngineImpl
import org.lsm4k.io.vfs.VirtualFileSystem
import org.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystem
import org.lsm4k.io.vfs.inmemory.InMemoryVirtualFileSystem
import org.lsm4k.util.report.DatabaseStructureReport
import java.io.File
import kotlin.time.Duration

/**
 * A single [DatabaseEngine] instance.
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
 * **Attention:** Closing a [DatabaseEngine] instance will also immediately terminate all
 * currently open transactions!
 *
 * **Note:** Usually, every program will only need one instance as it can support
 * several [Store]s simultaneously, but there is no inherent limitation
 * which would enforce this.
 */
interface DatabaseEngine : AutoCloseable {

    companion object {

        /**
         * Opens a new [DatabaseEngine] instance on the given [directory] with the given [configuration].
         *
         * If the directory is empty, a new database will be set up. Otherwise, the existing database will be opened.
         *
         * **Important:** There can be only **one** database instance for any given directory open at a time.
         *
         * **Important:** Do not change the contents of the given [directory] or its files (manually or otherwise) while a [DatabaseEngine]
         *                instance is operating on it. This may lead to arbitrary data corruptions and/or data loss!
         *
         * @param directory The directory to operate on.
         * @param configuration The configuration to use.
         *
         * @return The newly created [DatabaseEngine] instance. Must be closed explicitly via [DatabaseEngine.close]!
         */
        @JvmStatic
        @JvmOverloads
        fun openOnDirectory(directory: File, configuration: LSM4KConfiguration = LSM4KConfiguration()): DatabaseEngine {
            require(directory.exists() && directory.isDirectory) {
                "Argument 'directory' either doesn't exist or is not a directory: ${directory.absolutePath}"
            }
            val vfsConfig = configuration.createVirtualFileSystemConfiguration()
            val vfs = DiskBasedVirtualFileSystem(directory, vfsConfig)
            return openOnVirtualFileSystem(vfs, configuration)
        }

        /**
         * Opens a new [DatabaseEngine] in-memory instance with the given [configuration].
         *
         * **Important:** In-memory instances (as the name implies) do not spill any data to disk. Therefore, an in-memory
         *                instance can only hold as much data as the JVM heap size allows!
         *
         * **Important:** Since in-memory instances only operate on the JVM heap, all data will be erased once the
         *                instance is [closed][LSM4K.close] or when the JVM shuts down for any reason.
         *
         * @param configuration The configuration to use.
         *
         * @return The newly created [DatabaseEngine] instance. Must be closed explicitly via [DatabaseEngine.close]!
         */
        @JvmStatic
        @JvmOverloads
        fun openInMemory(configuration: LSM4KConfiguration = LSM4KConfiguration()): DatabaseEngine {
            val vfs = InMemoryVirtualFileSystem()
            return openOnVirtualFileSystem(vfs, configuration)
        }

        /**
         * Opens a new [DatabaseEngine] instance on the given [virtualFileSystem].
         *
         * This is an advanced method. Users should generally prefer [openOnDirectory] or [openInMemory] instead.
         *
         * The properties of the resulting instance will strongly depend on the underlying implementation of the given [virtualFileSystem].
         * Please refer to the documentation of the [virtualFileSystem] at hand for details.
         *
         * @param virtualFileSystem The [VirtualFileSystem] to use as the data storage.
         * @param configuration The configuration to use.
         *
         * @return The newly created [DatabaseEngine] instance. Must be closed explicitly via [DatabaseEngine.close]!
         *
         * @see [openOnDirectory]
         * @see [openInMemory]
         */
        @JvmStatic
        @JvmOverloads
        fun openOnVirtualFileSystem(virtualFileSystem: VirtualFileSystem, configuration: LSM4KConfiguration = LSM4KConfiguration()): DatabaseEngine {
            return DatabaseEngineImpl(virtualFileSystem, configuration)
        }

    }

    /**
     * Starts a new [TransactionMode.READ_ONLY] transaction.
     *
     * The transaction needs to be closed explicitly via [LSM4KTransaction.close] or [LSM4KTransaction.rollback].
     *
     * Read-only transactions do **not** support [LSM4KTransaction.commit] and attempting to call it will
     * result in an exception.
     *
     * Isolation Mode: **Snapshot**
     *
     * The following operations are allowed / not allowed:
     *
     * | Operation               | Example                                                                                                                           | Permitted |
     * |-------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|
     * | Read from Stores        | [store.get][TransactionalStore.get], [store.openCursor][TransactionalStore.openCursor]                                            | ✅ Yes     |
     * | Transient modifications | [store.put][TransactionalReadWriteStore.put], [store.delete][TransactionalReadWriteStore.delete]                                  | ✅ Yes     |
     * | Create stores           | [transaction.createNewStore][LSM4KTransaction.createNewStore], [store.deleteStore][TransactionalReadWriteStore.deleteStore] | ❌ No      |
     * | Commit changes          | [transaction.commit][LSM4KTransaction.commit]                                                                               | ❌ No      |
     *
     * Attempting to perform an action which is not permitted will result in an exception.
     *
     * While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:
     *
     * | Concurrent Transaction | Permitted |
     * |------------------------|-----------|
     * | Read-Only              | ✅ Yes     |
     * | Read-Write             | ✅ Yes     |
     * | Exclusive              | ✅ Yes     |
     *
     * @return The newly created transaction
     */
    fun beginReadOnlyTransaction(): LSM4KTransaction

    /**
     * Starts a new [TransactionMode.READ_WRITE] transaction.
     *
     * The transaction needs to be committed explicitly via [LSM4KTransaction.commit] or closed explicitly via [LSM4KTransaction.close] or [LSM4KTransaction.rollback].
     *
     * Isolation Mode: **Snapshot**
     *
     * The following operations are allowed / not allowed:
     *
     * | Operation               | Example                                                                                                                           | Permitted |
     * |-------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|
     * | Read from Stores        | [store.get][TransactionalStore.get], [store.openCursor][TransactionalStore.openCursor]                                            | ✅ Yes     |
     * | Transient modifications | [store.put][TransactionalReadWriteStore.put], [store.delete][TransactionalReadWriteStore.delete]                                  | ✅ Yes     |
     * | Create stores           | [transaction.createNewStore][LSM4KTransaction.createNewStore], [store.deleteStore][TransactionalReadWriteStore.deleteStore] | ✅ Yes     |
     * | Commit changes          | [transaction.commit][LSM4KTransaction.commit]                                                                               | ✅ Yes     |
     *
     * Attempting to perform an action which is not permitted will result in an exception.
     *
     * While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:
     *
     * | Concurrent Transaction | Permitted |
     * |------------------------|-----------|
     * | Read-Only              | ✅ Yes     |
     * | Read-Write             | ✅ Yes     |
     * | Exclusive              | ✅ No      |
     *
     *
     * This method uses the [LSM4KConfiguration.defaultLockAcquisitionTimeout].
     *
     * @return the newly created transaction
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the [LSM4KConfiguration.defaultLockAcquisitionTimeout].
     */
    fun beginReadWriteTransaction(): LSM4KTransaction

    /**
     * Starts a new [TransactionMode.READ_WRITE] transaction.
     *
     * The transaction needs to be committed explicitly via [LSM4KTransaction.commit] or closed explicitly via [LSM4KTransaction.close] or [LSM4KTransaction.rollback].
     *
     * Isolation Mode: **Snapshot**
     *
     * The following operations are allowed / not allowed:
     *
     * | Operation               | Example                                                                                                                           | Permitted |
     * |-------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|
     * | Read from Stores        | [store.get][TransactionalStore.get], [store.openCursor][TransactionalStore.openCursor]                                            | ✅ Yes     |
     * | Transient modifications | [store.put][TransactionalReadWriteStore.put], [store.delete][TransactionalReadWriteStore.delete]                                  | ✅ Yes     |
     * | Create stores           | [transaction.createNewStore][LSM4KTransaction.createNewStore], [store.deleteStore][TransactionalReadWriteStore.deleteStore] | ✅ Yes     |
     * | Commit changes          | [transaction.commit][LSM4KTransaction.commit]                                                                               | ✅ Yes     |
     *
     * Attempting to perform an action which is not permitted will result in an exception.
     *
     * While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:
     *
     * | Concurrent Transaction | Permitted |
     * |------------------------|-----------|
     * | Read-Only              | ✅ Yes     |
     * | Read-Write             | ✅ Yes     |
     * | Exclusive              | ✅ No      |
     *
     * @param lockAcquisitionTimeout If there is an [exclusive transaction][beginExclusiveTransaction] currently going on, this parameter determines the maximum duration to wait for.
     *                               If the exclusive transaction did not end before the duration runs out, a [TransactionLockAcquisitionException] will
     *                               be thrown. Use `null` to wait with no specified timeout.
     *
     * @return the newly created transaction
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the given [lockAcquisitionTimeout].
     */
    fun beginReadWriteTransaction(lockAcquisitionTimeout: Duration?): LSM4KTransaction


    /**
     * Starts a new [TransactionMode.EXCLUSIVE] transaction.
     *
     * The transaction needs to be committed explicitly via [LSM4KTransaction.commit] or closed explicitly via [LSM4KTransaction.close] or [LSM4KTransaction.rollback].
     *
     * Exclusive transactions enforce single-writer semantics and do not permit any concurrent [read-write][beginReadWriteTransaction] or [exclusive][beginExclusiveTransaction] transactions.
     *
     * Isolation Mode: **Serializable**
     *
     * The following operations are allowed / not allowed:
     *
     * | Operation               | Example                                                                                                                           | Permitted |
     * |-------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|
     * | Read from Stores        | [store.get][TransactionalStore.get], [store.openCursor][TransactionalStore.openCursor]                                            | ✅ Yes     |
     * | Transient modifications | [store.put][TransactionalReadWriteStore.put], [store.delete][TransactionalReadWriteStore.delete]                                  | ✅ Yes     |
     * | Create stores           | [transaction.createNewStore][LSM4KTransaction.createNewStore], [store.deleteStore][TransactionalReadWriteStore.deleteStore] | ✅ Yes     |
     * | Commit changes          | [transaction.commit][LSM4KTransaction.commit]                                                                               | ✅ Yes     |
     *
     * Attempting to perform an action which is not permitted will result in an exception.
     *
     * While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:
     *
     * | Concurrent Transaction | Permitted |
     * |------------------------|-----------|
     * | Read-Only              | ✅ Yes     |
     * | Read-Write             | ✅ No      |
     * | Exclusive              | ✅ No      |
     *
     * This method uses the [LSM4KConfiguration.defaultLockAcquisitionTimeout].
     *
     * @return the newly created transaction
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the [LSM4KConfiguration.defaultLockAcquisitionTimeout].
     */
    fun beginExclusiveTransaction(): LSM4KTransaction

    /**
     * Starts a new [TransactionMode.EXCLUSIVE] transaction.
     *
     * The transaction needs to be committed explicitly via [LSM4KTransaction.commit] or closed explicitly via [LSM4KTransaction.close] or [LSM4KTransaction.rollback].
     *
     * Exclusive transactions enforce single-writer semantics and do not permit any concurrent [read-write][beginReadWriteTransaction] or [exclusive][beginExclusiveTransaction] transactions.
     *
     * Isolation Mode: **Serializable**
     *
     * The following operations are allowed / not allowed:
     *
     * | Operation               | Example                                                                                                                           | Permitted |
     * |-------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|
     * | Read from Stores        | [store.get][TransactionalStore.get], [store.openCursor][TransactionalStore.openCursor]                                            | ✅ Yes     |
     * | Transient modifications | [store.put][TransactionalReadWriteStore.put], [store.delete][TransactionalReadWriteStore.delete]                                  | ✅ Yes     |
     * | Create stores           | [transaction.createNewStore][LSM4KTransaction.createNewStore], [store.deleteStore][TransactionalReadWriteStore.deleteStore] | ✅ Yes     |
     * | Commit changes          | [transaction.commit][LSM4KTransaction.commit]                                                                               | ✅ Yes     |
     *
     * Attempting to perform an action which is not permitted will result in an exception.
     *
     * While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:
     *
     * | Concurrent Transaction | Permitted |
     * |------------------------|-----------|
     * | Read-Only              | ✅ Yes     |
     * | Read-Write             | ✅ Yes     |
     * | Exclusive              | ✅ No      |
     *
     * @param lockAcquisitionTimeout If there is an [exclusive transaction][beginExclusiveTransaction] or [read-write transaction][beginReadWriteTransaction] currently going on,
     *                               this parameter determines the maximum duration to wait for.
     *                               If the exclusive transaction did not end before the duration runs out, a [TransactionLockAcquisitionException] will
     *                               be thrown. Use `null` to wait with no specified timeout.
     *
     * @return the newly created transaction
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the given [lockAcquisitionTimeout].
     */
    fun beginExclusiveTransaction(lockAcquisitionTimeout: Duration?): LSM4KTransaction

    /**
     * Same as [beginReadOnlyTransaction], but manages the transaction lifecycle automatically.
     *
     * This method accepts an [action] which receives the transaction as a parameter. The transaction will be
     * opened, passed to the action, and afterward the transaction will be [rolled back][LSM4KTransaction.rollback]
     * automatically if the action did not close it explicitly.
     *
     * Please refer to [beginReadOnlyTransaction] for details on the transaction semantics.
     *
     * @param action The action to perform on the transaction.
     *
     * @return The result of the [action], if any.
     */
    fun <T> readOnlyTransaction(action: (LSM4KTransaction) -> T): T {
        return this.beginReadOnlyTransaction().use(action)
    }

    /**
     * Same as [beginReadWriteTransaction], but manages the transaction lifecycle automatically.
     *
     * This method accepts an [action] which receives the transaction as a parameter. The transaction will be
     * opened, passed to the action, and afterward the transaction will be [rolled back][LSM4KTransaction.rollback]
     * automatically if the action did not close it explicitly.
     *
     * **Important:** If you wish to make the changes of the transaction permanent, you'll need
     * to call [LSM4KTransaction.commit] explicitly as part of the [action]!
     *
     * Please refer to [beginReadOnlyTransaction] for details on the transaction semantics.
     *
     * @param action The action to perform on the transaction.
     *
     * @return The result of the [action], if any.
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the [LSM4KConfiguration.defaultLockAcquisitionTimeout].
     */
    fun <T> readWriteTransaction(action: (LSM4KTransaction) -> T): T {
        return this.beginReadWriteTransaction().use(action)
    }

    /**
     * Same as [beginReadWriteTransaction], but manages the transaction lifecycle automatically.
     *
     * This method accepts an [action] which receives the transaction as a parameter. The transaction will be
     * opened, passed to the action, and afterward the transaction will be [rolled back][LSM4KTransaction.rollback]
     * automatically if the action did not close it explicitly.
     *
     * **Important:** If you wish to make the changes of the transaction permanent, you'll need
     * to call [LSM4KTransaction.commit] explicitly as part of the [action]!
     *
     * Please refer to [beginReadOnlyTransaction] for details on the transaction semantics.
     *
     * @param lockAcquisitionTimeout If there is an [exclusive transaction][beginExclusiveTransaction] or [read-write transaction][beginReadWriteTransaction] currently going on,
     *                               this parameter determines the maximum duration to wait for.
     *                               If the exclusive transaction did not end before the duration runs out, a [TransactionLockAcquisitionException] will
     *                               be thrown. Use `null` to wait with no specified timeout.
     * @param action The action to perform on the transaction.
     *
     * @return The result of the [action], if any.
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the given [lockAcquisitionTimeout].
     */
    fun <T> readWriteTransaction(lockAcquisitionTimeout: Duration?, action: (LSM4KTransaction) -> T): T {
        return this.beginReadWriteTransaction().use(action)
    }

    /**
     * Same as [beginExclusiveTransaction], but manages the transaction lifecycle automatically.
     *
     * This method accepts an [action] which receives the transaction as a parameter. The transaction will be
     * opened, passed to the action, and afterward the transaction will be [rolled back][LSM4KTransaction.rollback]
     * automatically if the action did not close it explicitly.
     *
     * **Important:** If you wish to make the changes of the transaction permanent, you'll need
     * to call [LSM4KTransaction.commit] explicitly as part of the [action]!
     *
     * Please refer to [beginReadOnlyTransaction] for details on the transaction semantics.
     *
     * @param action The action to perform on the transaction.
     *
     * @return The result of the [action], if any.
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the [LSM4KConfiguration.defaultLockAcquisitionTimeout].
     */
    fun <T> exclusiveTransaction(action: (LSM4KTransaction) -> T): T {
        return this.beginExclusiveTransaction().use(action)
    }

    /**
     * Same as [beginExclusiveTransaction], but manages the transaction lifecycle automatically.
     *
     * This method accepts an [action] which receives the transaction as a parameter. The transaction will be
     * opened, passed to the action, and afterward the transaction will be [rolled back][LSM4KTransaction.rollback]
     * automatically if the action did not close it explicitly.
     *
     * **Important:** If you wish to make the changes of the transaction permanent, you'll need
     * to call [LSM4KTransaction.commit] explicitly as part of the [action]!
     *
     * Please refer to [beginReadOnlyTransaction] for details on the transaction semantics.
     *
     * @param lockAcquisitionTimeout If there is an [exclusive transaction][beginExclusiveTransaction] or [read-write transaction][beginReadWriteTransaction] currently going on,
     *                               this parameter determines the maximum duration to wait for.
     *                               If the exclusive transaction did not end before the duration runs out, a [TransactionLockAcquisitionException] will
     *                               be thrown. Use `null` to wait with no specified timeout.
     * @param action The action to perform on the transaction.
     *
     * @return The result of the [action], if any.
     *
     * @throws TransactionLockAcquisitionException If there is a concurrent exclusive transaction going on for longer than the given [lockAcquisitionTimeout].
     */
    fun <T> exclusiveTransaction(lockAcquisitionTimeout: Duration?, action: (LSM4KTransaction) -> T): T {
        return this.beginExclusiveTransaction(lockAcquisitionTimeout).use(action)
    }

    /**
     * The root path of this instance.
     *
     * For disk-based implementations, this will return the path to the root directory where this instance operates.
     *
     * For in-memory implementations, this will return a synthetic path.
     */
    val rootPath: String

    /** Creates a read-only snapshot report about the structure of the database. */
    fun statusReport(): DatabaseStructureReport

    /**
     * Gets the statistics manager.
     */
    val statistics: StatisticsManager

}