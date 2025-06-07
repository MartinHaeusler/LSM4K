package io.github.martinhaeusler.lsm4k.test.util.fakestoredsl

import io.github.martinhaeusler.lsm4k.io.format.CompressionAlgorithm
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.Timestamp
import io.github.martinhaeusler.lsm4k.util.bloom.BytesBloomFilter
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize
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