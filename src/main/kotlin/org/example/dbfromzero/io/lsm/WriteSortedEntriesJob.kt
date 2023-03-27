package org.example.dbfromzero.io.lsm

import mu.KotlinLogging
import org.example.dbfromzero.io.stream.PositionTrackingStream
import org.example.dbfromzero.io.vfs.VirtualReadWriteFile
import org.example.dbfromzero.util.Bytes
import java.io.BufferedOutputStream

class WriteSortedEntriesJob(
    val name: String,
    val isBase: Boolean,
    val deltaIndex: Int,
    val indexRate: Int,
    val writes: Map<Bytes, Bytes>,
    val dataFile: VirtualReadWriteFile,
    val indexFile: VirtualReadWriteFile,
    val coordinator: DeltaWriterCoordinator,
) : Runnable {

    companion object {

        private val log = KotlinLogging.logger {}

        private const val IO_BUFFER_SIZE = 0x4000

    }

    override fun run() {
        var overWriter: VirtualReadWriteFile.OverWriter? = null
        var indexOverWriter: VirtualReadWriteFile.OverWriter? = null
        try {
            check(!dataFile.exists())
            check(!indexFile.exists())
            log.info { "Sorting ${writes.size} writes for '$name'" }
            val sortedEntries = writes.entries.sortedBy { it.key }

            overWriter = dataFile.createOverWriter()
            indexOverWriter = indexFile.createOverWriter()

            PositionTrackingStream(overWriter.outputStream, IO_BUFFER_SIZE).use { outputStream ->
                KeyValueFileWriter(BufferedOutputStream(indexOverWriter.outputStream, IO_BUFFER_SIZE)).use { indexWriter ->
                    val indexBuilder = IndexBuilder.create(indexWriter, indexRate)
                    KeyValueFileWriter(outputStream).use { writer ->
                        for (entry in sortedEntries) {
                            indexBuilder.accept(outputStream.position, entry.key)
                            writer.append(entry.key, entry.value)
                        }
                    }
                }
            }
            // TODO: this is potentially unsafe: System shutdown may result in partial write!
            overWriter.commit()
            indexOverWriter.commit()
            coordinator.commitWrites(this)
        } catch (e: Exception) {
            log.error(e) { "Error in writing sorted file for '${name}'!" }
            overWriter?.abort()
            indexOverWriter?.abort()
            coordinator.abortWrites(this)
        }
    }

}

