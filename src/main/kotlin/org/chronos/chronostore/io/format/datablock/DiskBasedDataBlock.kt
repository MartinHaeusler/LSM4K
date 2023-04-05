package org.chronos.chronostore.io.format.datablock

import com.google.common.hash.BloomFilter
import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.datablock.DataBlockUtil.seekInData
import org.chronos.chronostore.util.Bytes.Companion.mightContain
import org.chronos.chronostore.util.IOExtensions.withInputStream
import java.util.*

class DiskBasedDataBlock(
    override val metaData: BlockMetaData,
    private val bloomFilter: BloomFilter<ByteArray>,
    private val blockIndex: NavigableMap<KeyAndTimestamp, Int>,
    private val blockStartOffset: Long,
    private val blockEndOffset: Long,
) : DataBlock {

    private val blockSize = (blockEndOffset - blockStartOffset).toInt()

    init {
        require(blockStartOffset > 0) { "Argument 'blockStartOffset' (${blockStartOffset}) must not be negative!" }
        require(blockEndOffset > 0) { "Argument 'blockEndOffset' (${blockEndOffset}) must not be negative!" }
        require(blockStartOffset < blockEndOffset) { "Argument 'blockEndOffset' (${blockEndOffset}) must be larger than argument 'blockStartOffset' (${blockStartOffset})!" }
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

        val relativeSectionEnd = blockIndex.higherEntry(key)?.value ?: blockSize

        val absoluteSectionStart = this.blockStartOffset + relativeSectionStart
        val sectionLength = relativeSectionEnd - relativeSectionStart

        val bytes = driver.readBytes(absoluteSectionStart, sectionLength)
        bytes.withInputStream {
            return seekInData(it, key)
        }
    }


}