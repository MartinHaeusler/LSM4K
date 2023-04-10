package org.chronos.chronostore.io.format.datablock

import com.google.common.hash.BloomFilter
import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.datablock.DataBlockUtil.seekInData
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.mightContain
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.EmptyCursor
import org.chronos.chronostore.util.cursor.IndexBasedCursor
import java.util.*

class LazyDataBlock(
    override val metaData: BlockMetaData,
    private val bloomFilter: BloomFilter<ByteArray>,
    private val blockIndex: NavigableMap<KeyAndTimestamp, Int>,
    private val data: Bytes,
) : DataBlock {

    override fun isEmpty(): Boolean {
        return this.blockIndex.isEmpty()
    }

    override fun get(key: KeyAndTimestamp, driver: RandomFileAccessDriver): Pair<Command, Boolean>? {
        if (key.key !in metaData.minKey..metaData.maxKey) {
            return null
        }
        if (key.timestamp < metaData.minTimestamp) {
            return null
        }
        if (!bloomFilter.mightContain(key.key)) {
            // key isn't in our block
            return null
        }
        val (_, foundOffset) = blockIndex.floorEntry(key)
            ?: return null // request key is too small

        data.createInputStream(foundOffset).use { inputStream ->
            return seekInData(inputStream, key)
        }

    }

    override fun cursor(driver: RandomFileAccessDriver): Cursor<KeyAndTimestamp, Command> {
        // we have to read the entire block to create a cursor, there's no other choice.
        val commandList = mutableListOf<Command>()
        this.data.withInputStream { inputStream ->
            while (inputStream.available() > 0) {
                commandList += Command.readFromStream(inputStream)
            }
        }
        return if (commandList.isEmpty()) {
            EmptyCursor(
                getCursorName = { "Data Block #${this.metaData.blockSequenceNumber}" }
            )
        } else {
            IndexBasedCursor(
                minIndex = 0,
                maxIndex = commandList.size,
                getEntryAtIndex = { index ->
                    val command = commandList[index]
                    command.keyAndTimestamp to command
                },
                getCursorName = { "Data Block #${this.metaData.blockSequenceNumber}" }
            )
        }
    }

}