package org.chronos.chronostore.io.format

import org.chronos.chronostore.model.command.KeyAndTimestamp
import java.util.*

class IndexOfBlocks {

    private val startPositions: LongArray
    private val endOfLastBlock: Long
    private val minKeyAndTimestampToBlockIndex: TreeMap<KeyAndTimestamp, Int>

    @Suppress("ConvertSecondaryConstructorToPrimary")
    constructor(entries: List<Triple<Int, Long, KeyAndTimestamp>>, endOfLastBlock: Long) {
        val sortedEntries = entries.sortedBy { it.first }
        this.startPositions = LongArray(sortedEntries.size)
        this.endOfLastBlock = endOfLastBlock
        this.minKeyAndTimestampToBlockIndex = TreeMap()
        for (i in entries.indices) {
            val entry = sortedEntries[i]
            // we have to make sure that there are no duplicates and no "holes" in the indices
            if (entry.first != i) {
                throw IllegalArgumentException("Cannot build Index-Of-Blocks: the persistent format is missing block #${i}!")
            }
            this.startPositions[i] = entry.second
            this.minKeyAndTimestampToBlockIndex[entry.third] = entry.first
        }
    }

    val isEmpty: Boolean
        get(){
            return this.minKeyAndTimestampToBlockIndex.isEmpty()
        }

    val size: Int
        get() = this.minKeyAndTimestampToBlockIndex.size

    fun isValidBlockIndex(index: Int): Boolean {
        return index >= 0 && index <= this.startPositions.lastIndex
    }

    fun getBlockStartPositionAndLengthOrNull(blockIndex: Int): Pair<Long, Int>? {
        return when {
            blockIndex > this.startPositions.lastIndex -> {
                null
            }
            blockIndex == startPositions.lastIndex -> {
                val lastBlockStart = this.startPositions.last()
                val length = (this.endOfLastBlock - lastBlockStart).toInt()
                Pair(lastBlockStart, length)
            }
            else -> {
                val start = this.startPositions[blockIndex]
                val end = this.startPositions[blockIndex + 1]
                val length = (end - start).toInt()
                Pair(start, length)
            }
        }
    }

    fun getBlockIndexForKeyAndTimestampAscending(keyAndTimestamp: KeyAndTimestamp): Int? {
        val floorEntry = this.minKeyAndTimestampToBlockIndex.floorEntry(keyAndTimestamp)
            ?: return null // the key is too small to exist in this file
        return floorEntry.value.also {
            if(it > this.startPositions.lastIndex){
                throw IllegalStateException("KABOOM!")
            }
        }
    }

    fun getBlockIndexForKeyAndTimestampDescending(keyAndTimestamp: KeyAndTimestamp): Int? {
        val ceilEntry = this.minKeyAndTimestampToBlockIndex.ceilingEntry(keyAndTimestamp)
            ?: return null // the key is too large to exist in this file
        return ceilEntry.value
    }

}