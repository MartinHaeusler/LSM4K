package org.chronos.chronostore.io.format

import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.util.Bytes

class BlockIndexBuilder(
    /** The rate at which to index. Every n-th incoming command will be indexed.*/
    private val indexRate: Int
) {

    /**
     * The block index is a list of pairs:
     *  - each key contains the key and the timestamp of a command in the file.
     *  - each value contains the offset of that key (in bytes, offset 0 is start of block)
     */
    private val contents = mutableListOf<Pair<KeyAndTimestamp, Int>>()

    private var lastCommand: Command? = null
    private var lastCommandOffset: Int = 0
    private var wroteLastCommand = false
    private var commandIndex = 0

    private var closed = false

    fun visit(command: Command, blockOffset: Int) {
        check(!this.closed) { "This index builder has already been closed!" }
        if (commandIndex.mod(indexRate) == 0) {
            addToIndex(command, blockOffset)
            this.wroteLastCommand = true
        } else {
            this.wroteLastCommand = false
        }
        this.lastCommand = command
        this.lastCommandOffset = blockOffset
        commandIndex++
    }

    private fun addLastCommandToIndexIfNecessary() {
        val lastCommand = this.lastCommand
        if (this.wroteLastCommand || lastCommand == null) {
            // the last command is already in the index,
            // or we have seen no commands at all. Either
            // way, we cannot add anything to the index.
            return
        }
        this.addToIndex(lastCommand, this.lastCommandOffset)
    }

    fun build(): List<Pair<KeyAndTimestamp, Int>> {
        check(!this.closed) { "This index builder has already been closed!" }
        // make sure that the last pair is in the index
        this.addLastCommandToIndexIfNecessary()
        this.closed = true
        return this.contents
    }

    private fun addToIndex(command: Command, blockOffset: Int) {
        this.contents.add(Pair(command.keyAndTimestamp, blockOffset))
    }

}