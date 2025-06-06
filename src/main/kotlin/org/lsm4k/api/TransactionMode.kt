package org.lsm4k.api

/**
 * The mode of operation of a single [LSM4KTransaction].
 *
 * The mode is determined by the method which has been used to create the transaction:
 *
 * - [DatabaseEngine.beginReadOnlyTransaction] creates [TransactionMode.READ_ONLY] transactions
 * - [DatabaseEngine.beginReadWriteTransaction] creates [TransactionMode.READ_WRITE] transactions
 * - [DatabaseEngine.beginExclusiveTransaction] creates [TransactionMode.EXCLUSIVE] transactions
 *
 * The mode of a transaction cannot be changed after it has been created.
 */
enum class TransactionMode {

    /**
     * The transaction is a read-only transaction.
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
     */
    READ_ONLY,

    /**
     * The transaction is a read-write transaction.
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
     */
    READ_WRITE,

    /**
     * The transaction is an exclusive read-write transaction.
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
     * In addition, **no concurrent read-write (or exclusive) transactions are permitted**.
     *
     * While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:
     *
     * | Concurrent Transaction | Permitted |
     * |------------------------|-----------|
     * | Read-Only              | ✅ Yes     |
     * | Read-Write             | ❌ No      |
     * | Exclusive              | ❌ No      |
     */
    EXCLUSIVE,

}