package org.chronos.chronostore.io.format.datablock

import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.BlockMetaData
import java.util.*

class EagerDataBlock(
    override val metaData: BlockMetaData,
    private val data: NavigableMap<KeyAndTimestamp, Command>,
) : DataBlock {

    override fun get(key: KeyAndTimestamp, driver: RandomFileAccessDriver): Pair<Command, Boolean>? {
        if (key.key !in metaData.minKey..metaData.maxKey) {
            return null
        }
        if (key.timestamp < metaData.minTimestamp) {
            return null
        }
        val (foundKeyAndTimestamp, foundCommand) = data.floorEntry(key)
            ?: return null // request key is too small

        // did we hit the same key?
        return if (foundKeyAndTimestamp.key == key.key) {
            // key is the same -> this is the entry we're looking for.
            Pair(foundCommand, data.lastKey() == foundKeyAndTimestamp)
        } else {
            // key is different -> the key we wanted doesn't exist.
            null
        }
    }

}