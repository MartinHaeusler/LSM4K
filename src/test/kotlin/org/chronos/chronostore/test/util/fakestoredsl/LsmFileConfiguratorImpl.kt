package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.bloom.BytesBloomFilter
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import org.chronos.chronostore.util.unit.SizeUnit
import java.util.*

class LsmFileConfiguratorImpl(
    val index: FileIndex?,
) : LsmFileConfigurator {

    override var maxCompletelyWrittenTSN: TSN? = null

    override var minKey: Bytes? = null
        set(value) {
            field = value
            val max = this.maxKey
            if (value != null) {
                when {
                    max != null && value > max -> this.maxKey = value
                    max == null -> this.maxKey = value
                }
            }
        }

    override var maxKey: Bytes? = null
        set(value) {
            field = value
            val min = this.minKey
            if (value != null) {
                when {
                    min != null && value < min -> this.minKey = value
                    min == null -> this.minKey = value
                }
            }
        }

    override var minTSN: TSN? = null
        set(value) {
            field = value
            val max = this.maxTSN
            if (value != null) {
                when {
                    max != null && value > max -> this.maxTSN = value
                    max == null -> this.maxTSN = value
                }
            }
        }

    override var maxTSN: TSN? = null
        set(value) {
            field = value
            val min = this.minTSN
            if (value != null) {
                when {
                    min != null && value < min -> this.minTSN = value
                    min == null -> this.minTSN = value
                }
            }
        }

    override var firstKeyAndTSN: KeyAndTSN? = null
        set(value) {
            field = value
            val last = this.lastKeyAndTSN
            if (value != null) {
                when {
                    last != null && value > last -> this.lastKeyAndTSN = value
                    last == null -> this.lastKeyAndTSN = value
                }
            }
        }

    override var lastKeyAndTSN: KeyAndTSN? = null
        set(value) {
            field = value
            val first = this.firstKeyAndTSN
            if (value != null) {
                when {
                    first != null && value < first -> this.firstKeyAndTSN = value
                    first == null -> this.firstKeyAndTSN = value
                }
            }
        }

    override var headEntries: Long = 0

    override var totalEntries: Long = 0

    override var numberOfBlocks: Int = 0

    override var bloomFilter: BytesBloomFilter = BytesBloomFilter(100, 0.01)

    override var sizeOnDisk: BinarySize = 0.Bytes

    override var compression: CompressionAlgorithm = CompressionAlgorithm.SNAPPY

    override var maxBlockSize: BinarySize = BinarySize(1, SizeUnit.MEBIBYTE)

    override var fileUUID: UUID = UUID.randomUUID()

    override var numberOfMerges: Long = 0

    override var createdAt: Timestamp = System.currentTimeMillis()

}