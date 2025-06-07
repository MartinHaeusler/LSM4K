package io.github.martinhaeusler.lsm4k.test.cases.io.format

import io.github.martinhaeusler.lsm4k.io.format.CompressionAlgorithm
import io.github.martinhaeusler.lsm4k.io.format.FileMetaData
import io.github.martinhaeusler.lsm4k.io.format.LSMFileSettings
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.util.bloom.BytesBloomFilter
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.*

class FileMetaDataTest {

    @Test
    fun canSerializeAndDeserializeFileSettings() {
        val settings = LSMFileSettings(
            compression = CompressionAlgorithm.forCompressorName("snappy"),
            maxBlockSize = 8.MiB,
        )

        val serializedByteArray = ByteArrayOutputStream().use { baos ->
            settings.writeTo(baos)
            baos.toByteArray()
        }

        expectThat(settings.sizeBytes).isEqualTo(serializedByteArray.size)

        val deserialized = ByteArrayInputStream(serializedByteArray).use { bais ->
            LSMFileSettings.readFrom(bais)
        }

        expectThat(deserialized) {
            get { this.compression.compressor.uniqueName }.isEqualTo("snappy")
            get { this.maxBlockSize }.isEqualTo(8.MiB)
        }
    }

    @Test
    fun canSerializeAndDeserializeFileMetaData() {
        val fileMetaData = FileMetaData(
            settings = LSMFileSettings(
                compression = CompressionAlgorithm.forCompressorName("snappy"),
                maxBlockSize = 8.MiB,
            ),
            fileUUID = UUID.randomUUID(),
            minKey = Bytes.of("aaa"),
            maxKey = Bytes.of("zzz"),
            firstKeyAndTSN = KeyAndTSN(Bytes.of("aaa"), 100),
            lastKeyAndTSN = KeyAndTSN(Bytes.of("zzz"), 20),
            minTSN = 10,
            maxTSN = 1000,
            maxCompletelyWrittenTSN = 500,
            headEntries = 350,
            totalEntries = 400,
            numberOfBlocks = 20,
            numberOfMerges = 3,
            createdAt = System.currentTimeMillis(),
            bloomFilter = BytesBloomFilter(100, 0.001),
        )

        val serializedByteArray = ByteArrayOutputStream().use { baos ->
            fileMetaData.writeTo(baos)
            baos.toByteArray()
        }

        expectThat(fileMetaData.sizeBytes).isEqualTo(serializedByteArray.size.toLong())

        val deserialized = ByteArrayInputStream(serializedByteArray).use { bais ->
            FileMetaData.readFrom(bais)
        }

        expectThat(deserialized) {
            get { this.settings }.and {
                get { this.compression.compressor.uniqueName }.isEqualTo("snappy")
                get { this.maxBlockSize }.isEqualTo(8.MiB)
            }
            get { this.fileUUID }.isEqualTo(fileMetaData.fileUUID)
            get { this.minKey }.isNotNull().get { this.asString() }.isEqualTo("aaa")
            get { this.maxKey }.isNotNull().get { this.asString() }.isEqualTo("zzz")
            get { this.firstKeyAndTSN }.isNotNull().and {
                get { this.key.asString() }.isEqualTo("aaa")
                get { this.tsn }.isEqualTo(100)
            }
            get { this.lastKeyAndTSN }.isNotNull().and {
                get { this.key.asString() }.isEqualTo("zzz")
                get { this.tsn }.isEqualTo(20)
            }
            get { this.minTSN }.isEqualTo(10)
            get { this.maxTSN }.isEqualTo(1000)
            get { this.maxCompletelyWrittenTSN }.isEqualTo(500)
            get { this.headEntries }.isEqualTo(350)
            get { this.totalEntries }.isEqualTo(400)
            get { this.numberOfBlocks }.isEqualTo(20)
            get { this.numberOfMerges }.isEqualTo(3)
            get { this.createdAt }.isEqualTo(fileMetaData.createdAt)
            get { this.bloomFilter.toBytes().hex() }.isEqualTo(fileMetaData.bloomFilter.toBytes().hex())
        }
    }

}