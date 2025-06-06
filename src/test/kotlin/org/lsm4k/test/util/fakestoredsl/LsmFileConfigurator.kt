package org.lsm4k.test.util.fakestoredsl

import org.lsm4k.io.format.CompressionAlgorithm
import org.lsm4k.model.command.KeyAndTSN
import org.lsm4k.util.TSN
import org.lsm4k.util.Timestamp
import org.lsm4k.util.bloom.BytesBloomFilter
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.unit.BinarySize
import java.util.*

@FakeStoreDSL
interface LsmFileConfigurator {

    var sizeOnDisk: BinarySize

    var compression: CompressionAlgorithm

    var maxBlockSize: BinarySize

    var fileUUID: UUID

    var numberOfMerges: Long

    var createdAt: Timestamp

    var minKey: Bytes?

    var maxKey: Bytes?

    var firstKeyAndTSN: KeyAndTSN?

    var lastKeyAndTSN: KeyAndTSN?

    var minTSN: TSN?

    var maxTSN: TSN?

    var maxCompletelyWrittenTSN: TSN?

    var headEntries: Long

    var totalEntries: Long

    var numberOfBlocks: Int

    var bloomFilter: BytesBloomFilter



}