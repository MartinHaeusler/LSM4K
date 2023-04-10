package org.chronos.chronostore.lsm

import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.io.vfs.VirtualDirectory
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.ReentrantReadWriteLock

class LSMTree {

    private val id: UUID
    private val directory: VirtualDirectory
    private val mergeStrategy: MergeStrategy
    private val blockCache: BlockCache

    private val inMemoryTree = ConcurrentSkipListMap<KeyAndTimestamp, Command>()
    private val lock = ReentrantReadWriteLock(true)

    constructor(id: UUID, directory: VirtualDirectory, mergeStrategy: MergeStrategy, blockCache: BlockCache) {
        this.id = id
        this.directory = directory
        this.mergeStrategy = mergeStrategy
        this.blockCache = blockCache
    }


}
