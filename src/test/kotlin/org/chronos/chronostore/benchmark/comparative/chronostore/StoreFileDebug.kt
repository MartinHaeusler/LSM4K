package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.ChronoStoreFileFormat
import org.chronos.chronostore.io.format.ChronoStoreFileReader
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.util.bytes.BytesBuffer
import java.io.File

object StoreFileDebug {

    @JvmStatic
    fun main(args: Array<String>) {
        val inputDir = File("/home/martin/Documents/chronostore-test/taxonomyChronoStore")
        val vfs = DiskBasedVirtualFileSystem(inputDir, DiskBasedVirtualFileSystemSettings())

        for (i in 0..19) {
            val fileName = "${i.toString().padStart(10, '0')}.chronostore"
            val file = vfs.file("data/$fileName")
            try {
                val count = read(file)
                println("File ${fileName}: SUCCESS [${count}]")
            } catch (e: Exception) {
                println("File ${fileName}: FAILURE [${e}]")
            }

        }
//        val file = vfs.file("data/0000000000.chronostore")
//        read(file)


    }

    private fun read(file: VirtualReadWriteFile): Int {
        FileChannelDriver.Factory.createDriver(file).use { driver ->
            ChronoStoreFileReader(driver, LocalBlockCache.NONE).use { reader ->
                println("Total Entries: " + reader.fileHeader.metaData.totalEntries)
                println("Total Blocks: " + reader.fileHeader.indexOfBlocks.size)
                for (blockIndex in 0..<reader.fileHeader.indexOfBlocks.size) {
                    val startAndLength = reader.fileHeader.indexOfBlocks.getBlockStartPositionAndLengthOrNull(blockIndex)
                    if (startAndLength == null) {
                        println("  #${blockIndex}: <NO INFO>")
                    } else {
                        val (start, length) = startAndLength
                        println("  #${blockIndex}: ${start.toString().padStart(8)} | .. ${length.toString().padStart(8)} bytes .. | ${(start + length).toString().padStart(8)}")
                        val blockBytes = driver.readBytes(start, length)
                        val buffer = BytesBuffer(blockBytes)

                        val magicBytes = buffer.takeBytes(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.size)
                        if (magicBytes != ChronoStoreFileFormat.BLOCK_MAGIC_BYTES) {
                            throw IllegalArgumentException(
                                "Cannot read block from input: the magic bytes do not match!" +
                                    " Expected ${ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.hex()}, found ${magicBytes}!"
                            )
                        }
                        // read the individual parts of the binary format
                        buffer.takeLittleEndianInt() // block size; not needed here
                        val blockMetadataSize = buffer.takeLittleEndianInt()
                        val blockMetadataBytes = buffer.takeBytes(blockMetadataSize)

                        val bloomFilterSize = buffer.takeLittleEndianInt()
                        // skip the bloom filter, we don't need it for eager-loaded blocks
                        buffer.skipBytes(bloomFilterSize)

                        val compressedSize = buffer.takeLittleEndianInt()
                        val compressedBytes = buffer.skipBytes(compressedSize)

                        // deserialize the binary representations
                        val blockMetaData = blockMetadataBytes.createInputStream().use(BlockMetaData::readFrom)
                        println("    ${blockMetaData.commandCount} entries")

                    }
                }

                reader.openCursor().use { cursor ->
                    cursor.firstOrThrow()
                    return cursor.ascendingEntrySequenceFromHere().count()
                }
            }
        }
    }

}