package org.chronos.chronostore.io.format

import org.chronos.chronostore.model.command.KeyAndTSN
import java.util.*

class IndexOfBlocks {

    private val startPositions: LongArray
    private val endOfLastBlock: Long
    private val minKeyAndTSNToBlockIndex: TreeMap<KeyAndTSN, Int>

    @Suppress("ConvertSecondaryConstructorToPrimary")
    constructor(entries: List<Triple<Int, Long, KeyAndTSN>>, endOfLastBlock: Long) {
        val sortedEntries = entries.sortedBy { it.first }
        this.startPositions = LongArray(sortedEntries.size)
        this.endOfLastBlock = endOfLastBlock
        this.minKeyAndTSNToBlockIndex = TreeMap()
        for (i in entries.indices) {
            val entry = sortedEntries[i]
            // we have to make sure that there are no duplicates and no "holes" in the indices
            if (entry.first != i) {
                throw IllegalArgumentException("Cannot build Index-Of-Blocks: the persistent format is missing block #${i}!")
            }
            this.startPositions[i] = entry.second
            this.minKeyAndTSNToBlockIndex[entry.third] = entry.first
        }
    }

    val isEmpty: Boolean
        get() {
            return this.minKeyAndTSNToBlockIndex.isEmpty()
        }

    val size: Int
        get() = this.minKeyAndTSNToBlockIndex.size

    private var binarySizeCached: Long? = null

    val sizeInBytes: Long
        get(){
            val cached = binarySizeCached
            if(cached != null){
                return cached
            }
            val computedSize = computeBinarySizeUncached()
            this.binarySizeCached = computedSize
            return computedSize
        }

    private fun computeBinarySizeUncached(): Long{
        val startPositionsSize = startPositions.size * Long.SIZE_BYTES
        val treeSize = minKeyAndTSNToBlockIndex.entries.sumOf { (it.key.byteSize + Int.SIZE_BYTES).toLong() }
        return treeSize + startPositionsSize + Long.SIZE_BYTES
    }

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

    fun getBlockIndexForKeyAndTimestampAscending(keyAndTSN: KeyAndTSN): Int? {
        val floorEntry = this.minKeyAndTSNToBlockIndex.floorEntry(keyAndTSN)
            ?: return null // the key is too small to exist in this file
        return floorEntry.value
    }

    fun getBlockIndexForKeyAndTimestampDescending(keyAndTSN: KeyAndTSN): Int? {
        val ceilEntry = this.minKeyAndTSNToBlockIndex.ceilingEntry(keyAndTSN)
            ?: return null // the key is too large to exist in this file
        return ceilEntry.value
    }

}