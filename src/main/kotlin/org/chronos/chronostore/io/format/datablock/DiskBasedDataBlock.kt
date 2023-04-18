package org.chronos.chronostore.io.format.datablock

import com.google.common.hash.BloomFilter
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.datablock.DataBlockUtil.seekInData
import org.chronos.chronostore.util.Bytes.Companion.mightContain
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.EmptyCursor
import org.chronos.chronostore.util.cursor.IndexBasedCursor
import java.util.*

class DiskBasedDataBlock(
    override val metaData: BlockMetaData,
    private val bloomFilter: BloomFilter<ByteArray>,
    private val blockIndex: NavigableMap<KeyAndTimestamp, Int>,
    private val blockStartOffset: Long,
    private val blockEndOffset: Long,
    private val blockDataStartOffset: Long,
) : DataBlock {

    private val blockDataSize = (blockEndOffset - blockDataStartOffset).toInt()

    init {
        require(blockStartOffset > 0) { "Argument 'blockStartOffset' (${blockStartOffset}) must not be negative!" }
        require(blockEndOffset > 0) { "Argument 'blockEndOffset' (${blockEndOffset}) must not be negative!" }
        require(blockStartOffset < blockEndOffset) { "Argument 'blockEndOffset' (${blockEndOffset}) must be larger than argument 'blockStartOffset' (${blockStartOffset})!" }
        require(blockDataStartOffset <= blockEndOffset) { "Argument 'blockDataStartOffset' (${blockDataStartOffset}) must be less than or equal to argument 'blockEndOffset' (${blockEndOffset})!" }
    }

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
        val (_, relativeSectionStart) = blockIndex.floorEntry(key)
            ?: return null // request key is too small

        val relativeSectionEnd = blockIndex.higherEntry(key)?.value ?: blockDataSize

        val absoluteSectionStart = this.blockDataStartOffset + relativeSectionStart
        val sectionLength = relativeSectionEnd - relativeSectionStart

        val bytes = driver.readBytes(absoluteSectionStart, sectionLength)
        bytes.withInputStream {
            return seekInData(it, key)
        }
    }

    override fun cursor(driver: RandomFileAccessDriver): Cursor<KeyAndTimestamp, Command> {
        // we have to read the entire block to create a cursor, there's no other choice.
        val bytes = driver.readBytes(this.blockDataStartOffset, this.blockDataSize)
        val commandList = mutableListOf<Command>()
        bytes.withInputStream { inputStream ->
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
                maxIndex = commandList.lastIndex,
                getEntryAtIndex = { index ->
                    val command = commandList[index]
                    command.keyAndTimestamp to command
                },
                getCursorName = { "Data Block #${this.metaData.blockSequenceNumber}" }
            )
        }
    }

}