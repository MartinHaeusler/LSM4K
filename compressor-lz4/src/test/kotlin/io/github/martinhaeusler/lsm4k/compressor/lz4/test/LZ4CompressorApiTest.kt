package io.github.martinhaeusler.lsm4k.compressor.lz4.test

import io.github.martinhaeusler.lsm4k.compressor.api.Compressor
import io.github.martinhaeusler.lsm4k.compressor.api.Compressors
import io.github.martinhaeusler.lsm4k.compressor.lz4.Lz4Compressor
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import strikt.api.expectThat
import strikt.assertions.isA
import java.util.*
import kotlin.random.Random

class LZ4CompressorApiTest {

    @Test
    fun canCreateInstanceViaServiceLoader() {
        expectThat(getCompressor()).isA<Lz4Compressor>()
    }

    @Test
    fun canCompressAndDecompressBytes() {
        val compressor = getCompressor()
        repeat(100) {
            val randomData = Random.nextBytes(1024 * 10)
            val compressed = compressor.compress(randomData)
            val decompress = compressor.decompress(compressed)

            val originalHex = HexFormat.of().formatHex(randomData)
            val decompressedHex = HexFormat.of().formatHex(decompress)
            if (originalHex != decompressedHex) {
                fail(
                    "Compression/Decompression failed, results are not equal to original input!\n" +
                        "Original:     ${originalHex}" +
                        "Decompressed: ${decompressedHex}"
                )
            }
        }
    }

    private fun getCompressor(): Compressor {
        return Compressors.getCompressorForName(Lz4Compressor.UNIQUE_NAME)
    }

}