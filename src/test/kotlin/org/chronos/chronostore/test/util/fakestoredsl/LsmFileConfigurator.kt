package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.bloom.BytesBloomFilter
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.BinarySize
import java.util.*

@FakeStoreDSL
interface LsmFileConfigurator {

    var compression: CompressionAlgorithm

    var maxBlockSize: BinarySize

    var fileUUID: UUID

    var numberOfMerges: Long

    var createdAt: Timestamp

    var minKey: Bytes?

    var maxKey: Bytes?

    var minTSN: TSN?

    var maxTSN: TSN?

    var headEntries: Long

    var totalEntries: Long

    var numberOfBlocks: Int

    var bloomFilter: BytesBloomFilter


}