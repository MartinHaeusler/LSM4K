package org.lsm4k.impl.transaction

import org.lsm4k.api.Store
import org.lsm4k.model.command.Command
import org.lsm4k.util.TSN
import org.lsm4k.util.bytes.Bytes
import java.util.*

class TransactionBoundStoreContext(
    private val store: Store,
) {

    private val modifications = mutableMapOf<Bytes, Bytes?>()

    val allModifications: Map<Bytes, Bytes?>
        get() = Collections.unmodifiableMap(this.modifications)

    fun performPut(key: Bytes, value: Bytes) {
        this.modifications[key] = value
    }

    fun performDelete(key: Bytes) {
        this.modifications[key] = null
    }

    fun clearModifications() {
        this.modifications.clear()
    }

    fun isDirty(): Boolean {
        return this.modifications.isNotEmpty()
    }

    fun convertToCommands(commitTSN: TSN): Sequence<Command> {
        return this.modifications.asSequence().map { (key, value) ->
            if (value == null) {
                Command.del(key, commitTSN)
            } else {
                Command.put(key, commitTSN, value)
            }
        }
    }

    fun isKeyModified(key: Bytes): Boolean {
        return this.modifications.containsKey(key)
    }

    fun getLatest(key: Bytes): Bytes? {
        return this.modifications[key]
    }


}