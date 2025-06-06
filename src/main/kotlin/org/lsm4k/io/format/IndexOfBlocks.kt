package org.lsm4k.io.format

import org.lsm4k.model.command.KeyAndTSN
import java.util.*

class IndexOfBlocks {

    private val startPositions: LongArray
    private val endOfLastBlock: Long
    private val minKeyAndTSNToBlockIndex: TreeMap<KeyAndTSN, Int>

    private val firstKeyAndTSN: KeyAndTSN?
    private val lastKeyAndTSN: KeyAndTSN?

    @Suppress("ConvertSecondaryConstructorToPrimary")
    constructor(entries: List<Triple<Int, Long, KeyAndTSN>>, endOfLastBlock: Long, firstKeyAndTSN: KeyAndTSN?, lastKeyAndTSN: KeyAndTSN?) {
        val sortedEntries = entries.sortedBy { it.first }
        this.startPositions = LongArray(sortedEntries.size)
        this.endOfLastBlock = endOfLastBlock
        this.firstKeyAndTSN = firstKeyAndTSN
        this.lastKeyAndTSN = lastKeyAndTSN
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
        get() {
            val cached = binarySizeCached
            if (cached != null) {
                return cached
            }
            val computedSize = computeBinarySizeUncached()
            this.binarySizeCached = computedSize
            return computedSize
        }

    private fun computeBinarySizeUncached(): Long {
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
        val ceilEntry = this.minKeyAndTSNToBlockIndex.ceilingEntry(keyAndTSN)
        if (ceilEntry != null) {
            return ceilEntry.value
        }

        if (this.lastKeyAndTSN != null && keyAndTSN <= this.lastKeyAndTSN) {
            // the key may be contained in the last block
            return this.minKeyAndTSNToBlockIndex.lastEntry().value
        }

        // key is too big to be contained
        return null
    }

    fun getBlockIndexForKeyAndTimestampDescending(keyAndTSN: KeyAndTSN): Int? {
        val floorEntry = this.minKeyAndTSNToBlockIndex.floorEntry(keyAndTSN)
        if (floorEntry != null) {
            return floorEntry.value
        }

        if (this.firstKeyAndTSN != null && keyAndTSN >= this.firstKeyAndTSN) {
            // the key may be contained in the first block
            return this.minKeyAndTSNToBlockIndex.firstEntry().value
        }

        // the key is too small to exist in this file
        return null
    }


}