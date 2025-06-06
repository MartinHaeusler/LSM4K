package org.lsm4k.io.format.cursor

import org.lsm4k.io.format.BlockLoader
import org.lsm4k.io.format.FileHeader
import org.lsm4k.io.format.datablock.DataBlock
import org.lsm4k.io.vfs.VirtualFile
import org.lsm4k.util.collection.RingBuffer
import java.util.concurrent.CompletableFuture

class Prefetcher(
    val file: VirtualFile,
    val fileHeader: FileHeader,
    val blockLoader: BlockLoader,
    commandBufferSize: Int,
) {

    private val operationBuffer = RingBuffer<CursorMoveOperation>(commandBufferSize)

    private var nextCount = 0
    private var previousCount = 0

    private var prefetchTaskNextBlock: CompletableFuture<DataBlock?>? = null
    private var prefetchTaskPreviousBlock: CompletableFuture<DataBlock?>? = null

    private fun clearPrefetchState() {
        this.prefetchTaskNextBlock = null
        this.prefetchTaskPreviousBlock = null
        this.nextCount = 0
        this.previousCount = 0
        this.operationBuffer.clear()
    }

    fun getPreviousBlock(currentBlock: DataBlock): DataBlock? {
        val previous = this.prefetchTaskPreviousBlock
        val previousBlockIndex = currentBlock.metaData.blockSequenceNumber - 1
        val previousBlock = when {
            previous != null -> previous.get()
            previousBlockIndex < 0 -> null
            else -> blockLoader.getBlockOrNull(
                file = this.file,
                blockIndex = previousBlockIndex,
            )
        }
        this.clearPrefetchState()
        return previousBlock
    }

    fun getNextBlock(currentBlock: DataBlock): DataBlock? {
        val next = this.prefetchTaskNextBlock
        val nextBlockIndex = currentBlock.metaData.blockSequenceNumber + 1
        val nextBlock = when {
            next != null -> {
                next.get()
            }
            this.fileHeader.metaData.numberOfBlocks <= nextBlockIndex -> null
            else -> {
                this.blockLoader.getBlockOrNull(
                    file = this.file,
                    blockIndex = nextBlockIndex,
                )
            }
        }
        this.clearPrefetchState()
        return nextBlock
    }

    fun registerOperation(operation: CursorMoveOperation, currentBlock: DataBlock?) {
        if (!blockLoader.isAsyncSupported) {
            return
        }
        if (operation.isRandomJump) {
            // random jumps clear the operations buffer
            this.clearPrefetchState()
            return
        }
        val oldOperation = this.operationBuffer.add(operation)
        this.updateOperationCounters(oldOperation, -1)
        this.updateOperationCounters(operation, +1)

        if (operationBuffer.isFull) {
            // check if we have an indication for prefetching
            val direction = this.getPrimaryDirection()
            this.prefetchBlockIfNecessary(direction, currentBlock)
        }
    }

    private fun prefetchBlockIfNecessary(direction: NavigationDirection, currentBlock: DataBlock?) {
        when (direction) {
            NavigationDirection.ASCENDING -> prefetchNextBlock(currentBlock)
            NavigationDirection.DESCENDING -> prefetchPreviousBlock(currentBlock)
            // the cursor moves have been erratic and not predictable
            NavigationDirection.INDECISIVE -> return
        }
    }

    private fun prefetchPreviousBlock(currentBlock: DataBlock?) {
        if (this.prefetchTaskPreviousBlock != null) {
            // we're already prefetching
            return
        }
        // prefetch the next block
        val currentBlockIndex = currentBlock?.metaData?.blockSequenceNumber
            ?: return

        if (currentBlockIndex == 0) {
            return
        }

        val previousBlockIndex = currentBlockIndex - 1
        this.prefetchTaskNextBlock = blockLoader.getBlockAsync(this.file, previousBlockIndex)
    }

    private fun prefetchNextBlock(currentBlock: DataBlock?) {
        if (this.prefetchTaskNextBlock != null) {
            // we're already prefetching
            return
        }
        // prefetch the next block
        val currentBlockIndex = currentBlock?.metaData?.blockSequenceNumber
            ?: return

        val nextBlockIndex = currentBlockIndex + 1

        if (nextBlockIndex >= this.fileHeader.metaData.numberOfBlocks) {
            return
        }

        this.prefetchTaskNextBlock = this.blockLoader.getBlockAsync(this.file, nextBlockIndex)
    }

    private fun getPrimaryDirection(): NavigationDirection {
        val threshold = 0.75 * this.operationBuffer.size
        if (this.nextCount > threshold) {
            return NavigationDirection.ASCENDING
        }
        if (this.previousCount > threshold) {
            return NavigationDirection.DESCENDING
        }
        return NavigationDirection.INDECISIVE
    }

    private fun updateOperationCounters(operation: CursorMoveOperation?, delta: Int) {
        when (operation) {
            CursorMoveOperation.NEXT -> this.nextCount += delta
            CursorMoveOperation.PREVIOUS -> this.previousCount += delta
            else -> return // no-op
        }
    }

    private enum class NavigationDirection {

        ASCENDING,

        DESCENDING,

        INDECISIVE

    }
}