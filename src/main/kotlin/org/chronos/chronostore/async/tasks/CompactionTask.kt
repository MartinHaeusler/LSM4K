package org.chronos.chronostore.async.tasks

import com.google.common.collect.Iterators
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileReader
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.ChronoStoreFileWriter
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.lsm.BlockCache
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.cursor.Cursor
import java.io.IOException

class CompactionTask(
    private val filesToCompact: List<VirtualReadWriteFile>,
    private val target: VirtualReadWriteFile,
    private val targetFileSettings: ChronoStoreFileSettings,
    private val targetMetadata: Map<Bytes, Bytes>,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val blockReadMode: BlockReadMode,
    private val blockCache: BlockCache,
    override val name: String
) : AsyncTask {

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted(this.name)
        val readers = mutableListOf<ChronoStoreFileReader>()
        val cursors = mutableListOf<Cursor<KeyAndTimestamp, Command>>()
        try {
            monitor.subTask(0.1, "Preparing Resources") {
                prepareResources(readers, cursors)
            }
            if (cursors.isEmpty()) {
                // we've got nothing to merge, we're done.
                return
            }
            // we're going from sequences to iterators here, because Guava has a really nice "merge sorted" iterator.
            val iterators = cursors.map { it.ascendingValueSequenceFromHere().iterator() }
            val jointIterator = Iterators.mergeSorted(iterators, Comparator.naturalOrder())
            this.target.withOverWriter { overWriter ->
                monitor.subTask(0.7, "Compacting ${cursors.size} files") {
                    ChronoStoreFileWriter(
                        outputStream = overWriter.outputStream,
                        settings = this.targetFileSettings,
                        metadata = this.targetMetadata
                    ).use { writer ->
                        writer.writeFile(jointIterator)
                    }
                }
                monitor.subTask(0.1, "Flushing changes to disk") {
                    overWriter.commit()
                }
            }
        } finally {
            closeResources(cursors, readers)
        }
        monitor.subTask(0.1, "Cleaning up old files") {
            // delete the existing files
            deleteOldFiles()
        }
    }

    private fun deleteOldFiles() {
        for (file in this.filesToCompact) {
            file.deleteOverWriterFileIfExists()
            file.delete()
        }
    }

    private fun prepareResources(
        readers: MutableList<ChronoStoreFileReader>,
        cursors: MutableList<Cursor<KeyAndTimestamp, Command>>
    ) {
        for (file in filesToCompact) {
            readers += ChronoStoreFileReader(this.driverFactory.createDriver(file), this.blockReadMode, this.blockCache)
        }
        for (reader in readers) {
            val cursor = reader.openCursor()
            if (cursor.first()) {
                cursors += cursor
            } else {
                cursor.close()
            }
        }
    }

    private fun closeResources(
        cursors: MutableList<Cursor<KeyAndTimestamp, Command>>,
        readers: MutableList<ChronoStoreFileReader>
    ) {
        val suppressedExceptions = mutableListOf<Exception>()
        for (cursor in cursors) {
            try {
                cursor.close()
            } catch (e: Exception) {
                suppressedExceptions.add(e)
            }
        }
        for (reader in readers) {
            try {
                reader.close()
            } catch (e: Exception) {
                suppressedExceptions.add(e)
            }
        }
        if (suppressedExceptions.isNotEmpty()) {
            throw IOException("An error occurred while closing the files during a compaction.").also { ioEx ->
                suppressedExceptions.forEach(ioEx::addSuppressed)
            }
        }
    }


}