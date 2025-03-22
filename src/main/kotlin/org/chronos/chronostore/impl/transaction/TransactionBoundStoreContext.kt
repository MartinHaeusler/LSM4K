package org.chronos.chronostore.impl.transaction

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.bytes.Bytes
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