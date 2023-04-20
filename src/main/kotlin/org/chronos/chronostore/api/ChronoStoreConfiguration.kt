package org.chronos.chronostore.api

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.lsm.MergeStrategy

class ChronoStoreConfiguration {

    var maxWriterThreads: Int = 5

    var blockReadMode: BlockReadMode = BlockReadMode.IN_MEMORY_EAGER

    var randomFileAccessDriverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory

    var mergeStrategy: MergeStrategy = TODO("create a default strategy")

}