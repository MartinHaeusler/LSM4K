package io.github.martinhaeusler.lsm4k.wal

import com.google.common.io.CountingOutputStream
import io.github.martinhaeusler.lsm4k.api.exceptions.WriteAheadLogCorruptedException
import io.github.martinhaeusler.lsm4k.io.structure.LSM4KStructure.WRITE_AHEAD_LOG_FILE_PREFIX
import io.github.martinhaeusler.lsm4k.io.structure.LSM4KStructure.WRITE_AHEAD_LOG_FILE_SUFFIX
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.ManagerState
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.StreamExtensions.byteCounting
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.report.WalFileReport
import io.github.martinhaeusler.lsm4k.util.report.WalReport
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import io.github.martinhaeusler.lsm4k.wal.format.TransactionCommandEntry
import io.github.martinhaeusler.lsm4k.wal.format.TransactionCommitEntry
import io.github.martinhaeusler.lsm4k.wal.format.TransactionStartEntry
import io.github.martinhaeusler.lsm4k.wal.format.WALEntry
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.Volatile
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

/**
 * The [WriteAheadLog] class manages the Write-Ahead-Log (WAL) of the store.
 *
 * The WAL is stored in a single [directory] and can consist of multiple files.
 * Each file is written in an append-only fashion and consists of a plain series of
 * entries. No additional headers or footers are used. For a description of the entry
 * format, please see [WALEntry].
 *
 * Each file has a maximum size given by [maxWalFileSizeBytes] (please note that this
 * is not a hard limit; no further data will be stored in a WAL file **after** this
 * size has been reached, but the last entry appended to a file may exceed the limit
 * by its own binary length).
 *
 * When a WAL file becomes full, the next WAL file in sequence is opened; this
 * is sometimes referred to as "log segmentation". The main purpose of this
 * behavior is to easily delete old "chunks" of the WAL which are not needed
 * anymore because all of their content has already been (persistently) written
 * to the actual stores.
 *
 * In order to determine which WAL files can be discarded, we consider the
 * largest transaction TSN which has been (persistently) written by all
 * stores. This is called the "low watermark". If the transaction with the
 * highest TSN in a WAL file is less than or equal to the low watermark,
 * we can safely discard the WAL file because all of its data has already been
 * transferred to and been persisted by the actual stores.
 */
class WriteAheadLog(
    /** The directory where the WAL files are stored. */
    private val directory: VirtualDirectory,
    /** The maximum size for a single WAL file (soft limit). */
    private val maxWalFileSizeBytes: Long = 128.MiB.bytes,
    /** The minimum number of WAL files to keep before shortening the WAL. */
    private val minNumberOfFiles: Int = 1,
) : AutoCloseable {

    companion object {

        private val log = KotlinLogging.logger {}

        private val FILE_NAME_REGEX = """${Regex.escape(WRITE_AHEAD_LOG_FILE_PREFIX)}(\d+)${Regex.escape(WRITE_AHEAD_LOG_FILE_SUFFIX)}""".toRegex()

        private val SHORTENED_WAL_SUMMARY_FILE = "${WRITE_AHEAD_LOG_FILE_PREFIX}Base${WRITE_AHEAD_LOG_FILE_SUFFIX}"

    }

    private val lock = ReentrantReadWriteLock(true)
    private val shorteningLock = ReentrantLock(true)

    @Volatile
    private var state: ManagerState = ManagerState.OPEN
    private val walSummaryFile: WALSummaryFile
    private val walFiles = mutableListOf<WALFile>()

    init {
        require(maxWalFileSizeBytes > 0) { "Argument 'maxWalFileSizeBytes' (${maxWalFileSizeBytes}) must be positive!" }
        require(minNumberOfFiles > 0) { "Argument 'minNumberOfFiles' (${minNumberOfFiles}) must be positive!" }
        if (!this.directory.exists()) {
            this.directory.mkdirs()
        }

        this.walSummaryFile = WALSummaryFile(this.directory.file(SHORTENED_WAL_SUMMARY_FILE))

        this.directory.listFiles().asSequence()
            .mapNotNull(::createWALFileOrNull)
            .sortedBy { it.sequenceNumber }
            .forEach(walFiles::add)

        // ensure that no WAL files are missing
        this.checkNoWalFileMissing()
    }

    private fun checkNoWalFileMissing() {
        this.checkWALSummaryFile()

        // all the files must be present, in sequence
        var previous = -1L
        for (walFile in this.walFiles) {
            if (previous < 0) {
                previous = walFile.sequenceNumber
                continue
            }
            if (walFile.sequenceNumber != previous + 1) {
                throw WriteAheadLogCorruptedException("WAL file ${previous} is present, but ${previous + 1} is missing! The next present WAL file is ${walFile.sequenceNumber}!")
            }
            previous = walFile.sequenceNumber
        }
    }

    private fun checkWALSummaryFile() {
        // the WAL Summary contains the highest WAL file sequence number we have dropped
        // because of WAL shortening.
        val highestDroppedWalSequenceNumber = this.walSummaryFile.getHighestDroppedWalSequenceNumber()

        val firstWalFileSequenceNumber = this.walFiles.firstOrNull()?.sequenceNumber
        when {
            firstWalFileSequenceNumber == null && highestDroppedWalSequenceNumber == null -> {
                // we have no WAL files yet, and the summary says that there are no WAL files. That's ok.
                return
            }

            firstWalFileSequenceNumber == null && highestDroppedWalSequenceNumber != null -> {
                // we have dropped WAL files, but no WAL files left? That's definitely a problem.
                throw WriteAheadLogCorruptedException("Write Ahead Log files on disk do not include file '${highestDroppedWalSequenceNumber + 1}' !")
            }

            firstWalFileSequenceNumber != null && highestDroppedWalSequenceNumber == null -> {
                // we have WAL files, but we've never dropped any
                // -> the first WAL file must have sequence number zero.
                if (firstWalFileSequenceNumber != 0L) {
                    throw WriteAheadLogCorruptedException("Write Ahead Log files on disk do not include file '${createWalFileName(0)}'!")
                } else {
                    // WAL was never shortened, but it starts with file 0, so that's ok.
                    return
                }
            }

            firstWalFileSequenceNumber != null && highestDroppedWalSequenceNumber != null -> {
                if (firstWalFileSequenceNumber > highestDroppedWalSequenceNumber + 1) {
                    // we're missing a file!
                    throw WriteAheadLogCorruptedException("Write Ahead Log files on disk do not include file '${highestDroppedWalSequenceNumber + 1}' !")
                } else {
                    // all good. Note that this case also includes the edge case where
                    // we've persisted that a WAL file was dropped, but we didn't actually
                    // drop it yet because the process was interrupted.
                    return
                }
            }
        }
        throw IllegalStateException("Unreachable code!")
    }

    private fun createWALFileOrNull(file: VirtualFile): WALFile? {
        if (file !is VirtualReadWriteFile) {
            return null
        }
        val match = FILE_NAME_REGEX.matchEntire(file.name)
            ?: return null
        val sequenceNumber = match.groups[1]?.value?.toLong()
            ?: return null
        return WALFile(file, sequenceNumber)
    }


    fun performStartupRecoveryCleanup(maxPersistedTSN: TSN) {
        this.state.checkOpen()
        // 1. find out in which wal file (going backwards, starting from last) the latest COMPLETED transaction starts.
        // 2. delete all entries after that commit to get rid of partially committed data.
        // 3. if a wal file becomes empty due to this operation, delete the entire wal file.
        this.lock.write {
            if (this.walFiles.isEmpty()) {
                // we don't have any WAL files yet.
                return
            }

            val lastCommit = this.dropLatestWalFilesUntilCommitEntryIsFound()
            if (lastCommit == null) {
                if (maxPersistedTSN > 0) {
                    throw WriteAheadLogCorruptedException("There is no WAL file which reflects persisted transaction with Serial Number ${maxPersistedTSN}!")
                }
                // there is no WAL to replay, store is empty, all good.
                return
            }

            if (lastCommit < maxPersistedTSN) {
                throw WriteAheadLogCorruptedException("There is no WAL file which reflects persisted transaction with Serial Number ${maxPersistedTSN}!")
            }

            // iterate through the final file again, find the exact (binary) offset position of that
            // commit, and truncate the file after that.
            val lastFile = this.walFiles.last()

            // always fsync WAL files before reading them. It could happen that:
            // - we write to a WAL file (but we don't reach the sync point)
            // - the process (not the OS) gets killed half-way
            // - the process gets restarted on the same directory
            // - the process now reads the half-written file FROM THE OS CACHE
            // - the process acts upon the data
            // - power goes out
            // -> We've read from a WAL which is no longer there :BOOM:
            //
            // Solution: play it safe, fsync the file before reading it.
            lastFile.file.fsync()

            val offset = this.getOffsetOfCommitEntry(lastFile, lastCommit)
            lastFile.file.truncateAfter(offset)
        }
    }


    private fun dropLatestWalFilesUntilCommitEntryIsFound(): TSN? {
        while (this.walFiles.isNotEmpty()) {
            val lastFile = this.walFiles.last()

            // always fsync WAL files before reading them. It could happen that:
            // - we write to a WAL file (but we don't reach the sync point)
            // - the process (not the OS) gets killed half-way
            // - the process gets restarted on the same directory
            // - the process now reads the half-written file FROM THE OS CACHE
            // - the process acts upon the data
            // - power goes out
            // -> We've read from a WAL which is no longer there :BOOM:
            //
            // Solution: play it safe, fsync the file before reading it.
            lastFile.file.fsync()

            val lastFileTransactionInfo = this.getTransactionInfo(lastFile, ignoreTruncatedEntries = true)
            val maxCompletedTSN = lastFileTransactionInfo.maxCompletedTSN
            if (maxCompletedTSN == null) {
                // there's NO commit in the latest file? Delete it, and repeat the process
                // with the previous file until we find one which contains a commit
                this.walFiles.removeLast()
                lastFile.delete()
                continue
            }
            return maxCompletedTSN
        }
        return null
    }

    private fun getOffsetOfCommitEntry(walFile: WALFile, commitTsn: TSN): Long {
        walFile.inputStream()
            .buffered()
            .byteCounting()
            .use { inputStream ->
                while (true) {
                    val currentEntry = WALEntry.readFrom(inputStream)
                        ?: break
                    if (currentEntry is TransactionCommitEntry && currentEntry.commitTSN == commitTsn) {
                        // everything after this position is trash and needs to be removed
                        return inputStream.count
                    }
                }
                // reached the end of the file
                throw IllegalStateException("Could not find commit entry for TSN ${commitTsn} in Write-Ahead-Log file '${walFile.file.path}'!")
            }
    }

    fun getLatestFileSequenceNumber(): Long? {
        this.state.checkOpen()
        this.lock.read {
            return this.walFiles.lastOrNull()?.sequenceNumber
        }
    }

    fun deleteWalFilesWithSequenceNumberLowerThan(sequenceNumber: Long) {
        this.state.checkOpen()
        // only allow a single shortening operation at any point in time.
        this.shorteningLock.withLock {
            val fileToKeep: WALFile?
            val earlierFiles: List<WALFile>
            this.lock.read {
                fileToKeep = this.walFiles.asSequence().filter { it.sequenceNumber <= sequenceNumber }.maxByOrNull { it.sequenceNumber }
                earlierFiles = this.walFiles.filter { it.sequenceNumber < (fileToKeep?.sequenceNumber ?: sequenceNumber) }
            }
            log.debug { "Checkpoint: WAL file to keep: ${fileToKeep?.sequenceNumber}, earlier files: ${earlierFiles.joinToString { it.sequenceNumber.toString() }}" }
            if (fileToKeep == null) {
                // the desired file doesn't exist -> nothing we can do...
                log.debug { "Checkpoint: No file to keep, can't shorten WAL." }
                return
            }

            if (earlierFiles.isEmpty()) {
                // there ARE no earlier files -> we're done.
                log.debug { "Checkpoint: No earlier files, can't shorten WAL." }
                return
            }

            // we run into a problem here:
            //
            // - The fileToKeep may not start with a "begin transaction" entry, it may
            //   be a continuation of the previous WAL file.
            //
            // - If it *is* a continuation of the previous WAL file, we have to KEEP the
            //   previous file as well to ensure that we can replay the transaction on
            //   startup.
            //
            // - The previous file may not contain the "begin transaction" command either,
            //   so we have to keep looking backwards until we find it, and we must not
            //   delete ANY of those files.
            val partialCommitTsn = this.getCommitTsnOfPartialTransactionAtStartOfFile(fileToKeep)
            if (partialCommitTsn == null) {
                this.lock.write {
                    // delete all earlier files, we don't need them anymore.
                    log.debug { "Checkpoint: No partial commit TSN found in WAL ${fileToKeep.sequenceNumber}, proceeding to delete files ${earlierFiles.joinToString { it.sequenceNumber.toString() }}" }

                    this.walSummaryFile.setHighestDroppedWalSequenceNumber(earlierFiles.maxOf { it.sequenceNumber })
                    this.walFiles.removeAll(earlierFiles)
                    earlierFiles.forEach(WALFile::delete)
                }
                return
            }

            // we now try to find the file which started this transaction. To do so,
            // we walk the list backwards while keeping track of the file index.
            val partialCommitStartWALFileIndex = this.getPartialCommitStartWALFileIndex(
                fileWithTransactionCommit = fileToKeep,
                partialCommitTsn = partialCommitTsn,
                earlierFiles = earlierFiles,
            )

            // delete all earlier files.
            val filesToDelete = earlierFiles.subList(
                fromIndex = 0,
                // toIndex is EXCLUSIVE, which is exactly what we want here because we
                // want to keep the file with that index.
                toIndex = partialCommitStartWALFileIndex
            )
            log.debug { "Checkpoint: Due to partial commit in WAL ${fileToKeep.sequenceNumber}, only the following files will be deleted: ${filesToDelete.joinToString { it.sequenceNumber.toString() }}" }

            if (filesToDelete.isNotEmpty()) {
                this.lock.write {
                    this.walSummaryFile.setHighestDroppedWalSequenceNumber(filesToDelete.maxOf { it.sequenceNumber })
                    this.walFiles.removeAll(filesToDelete)
                    filesToDelete.forEach(WALFile::delete)
                }
            }
        }
    }

    private fun getCommitTsnOfPartialTransactionAtStartOfFile(walFile: WALFile): TSN? {
        walFile.inputStream().buffered().use { bufferedInput ->
            return when (val entry = WALEntry.readStreaming(bufferedInput).firstOrNull()) {
                null -> {
                    // there are no entries in this file.
                    null
                }

                is TransactionStartEntry -> {
                    // the file starts with a new transaction and has
                    // no dependency on the previous file at all. There
                    // is no partial transaction at the start of the file.
                    null
                }

                else -> {
                    // the first entry in the file is a continuation
                    // of a previous file, so we do have a dependency.
                    entry.commitTSN
                }
            }
        }
    }

    private fun getPartialCommitStartWALFileIndex(fileWithTransactionCommit: WALFile, partialCommitTsn: TSN, earlierFiles: List<WALFile>): Int {
        for (earlierWalFileIndex in earlierFiles.indices.reversed()) {
            val file = earlierFiles[earlierWalFileIndex]
            val txStartInfo = this.getTransactionInfo(file, ignoreTruncatedEntries = false)
            if (txStartInfo.minStartTransactionTSN == null) {
                // there were no transaction starts in this file, the transaction
                // has started earlier -> check the previous file.
                continue
            }
            if (txStartInfo.maxUncompletedTransactionStartTSN == null) {
                // the previous file completed all of its transactions? Weird.
                // Then where does our partial transaction start?
                throw WriteAheadLogCorruptedException(
                    "WAL File '${fileWithTransactionCommit.file.path}' starts with a partial transaction (TSN: ${partialCommitTsn}). However, the " +
                        "previous file '${file.file.path}' only contains completed transactions!"
                )
            }
            if (txStartInfo.maxUncompletedTransactionStartTSN != partialCommitTsn) {
                // the previous file ended with a different partial transaction
                // than the one we're looking for? Then where did our transaction start?
                throw WriteAheadLogCorruptedException(
                    "WAL File '${fileWithTransactionCommit.file.path}' starts with a partial transaction (TSN: ${partialCommitTsn}). However, the " +
                        "previous file '${earlierWalFileIndex}' ends with a different partial transaction (TSN: ${partialCommitTsn})!"
                )
            }

            // we've located the beginning of our partial transaction.
            return earlierWalFileIndex
        }

        // we went through all the files we have, but we still couldn't find our transaction start?
        throw WriteAheadLogCorruptedException(
            "WAL File '${fileWithTransactionCommit.file.path}' starts with a partial transaction (TSN: ${partialCommitTsn})." +
                "However, none of the ${earlierFiles.size} earlier files contains the corresponding transaction start entry!"
        )
    }

    /**
     * Adds a commit to the Write Ahead Log file.
     *
     * This operation requires the write lock on the WAL file; it may be blocked until
     * concurrent reads or writes have been completed.
     *
     * @param commitTSN The transaction commit [TSN].
     * @param transactionChanges A (potentially lazy) sequence of pairs, each containing a [StoreId] and a [Command]
     * applied to that store by the transaction. This method will consume the sequence in a lazy (streaming) fashion.
     */
    fun addCommittedTransaction(commitTSN: TSN, transactionChanges: Sequence<Pair<StoreId, Command>>) {
        check(commitTSN >= 0) { "Argument 'commitTSN' (${commitTSN}) must not be negative!" }
        this.state.checkOpen()
        this.lock.write {
            // TODO [FEATURE]: Compress WAL files by writing blocks of fixed size within each file and compress the blocks.

            // the full sequence of entries consists of:
            // - start transaction
            // - changes
            // - commit transaction
            val entrySequence = sequenceOf(TransactionStartEntry(commitTSN)) +
                transactionChanges.map { TransactionCommandEntry(it.first, it.second) } +
                sequenceOf(TransactionCommitEntry(commitTSN))

            // note that we've got TWO while(...) loops that are both based on
            // the iterator. The reason is the following:
            //
            // - the outer while loop prepares the target file. When it's full, it writes
            //   the checksum and gets the next one. The outer loop NEVER advances the iterator.
            //
            // - the inner loop actually advances the iterator and pipes the entries into the
            //   output stream of the target file, while keeping track of the file size. If the
            //   file is full, it breaks the inner loop.
            //
            // So while this algorithm may LOOK like it's quadratic complexity, it's actually
            // linear and streaming (i.e. it never reads all entries of the sequence into memory
            // at the same time).
            val iterator = entrySequence.iterator()
            while (iterator.hasNext()) { // outer loop
                this.state.checkOpen()
                // open the most recent WAL file which still has some space left
                val targetFile = this.getOrCreateTargetWALFile()
                // how much space is left in the file?
                val freeBytes = this.maxWalFileSizeBytes - targetFile.length
                // safeguard, should never happen
                check(freeBytes > 0) { "Current WAL file has no free space!" }

                var isTargetFileFull = false
                targetFile.append { outputStream ->
                    CountingOutputStream(outputStream).use { countingOutputStream ->
                        while (iterator.hasNext()) { // inner loop
                            this.state.checkOpen()
                            val entry = iterator.next()
                            entry.writeTo(countingOutputStream)
                            if (freeBytes - countingOutputStream.count <= 0) {
                                // the file is full. Flush the stream and break
                                // the inner loop. This will cause the outer loop
                                // to advance to the next file.
                                countingOutputStream.flush()
                                isTargetFileFull = true
                                break
                            }
                        }
                        // no more entries!
                        countingOutputStream.flush()
                    }
                }
                if (isTargetFileFull) {
                    // we're done with the file, create the checksum
                    targetFile.createChecksumFileIfNecessary()
                }
            }
        }
    }

    private fun getOrCreateTargetWALFile(): WALFile = this.lock.write {
        val currentWALFile = this.walFiles.lastOrNull()
            ?: return createAndRegisterNewWALFile(newSequenceNumber = 0) // we have no WAL file, start at sequence number 0

        return if (!currentWALFile.isFull(this.maxWalFileSizeBytes)) {
            // we still have room in our current file.
            currentWALFile
        } else {
            // current file has no more room, create a new one
            val newSequenceNumber = currentWALFile.sequenceNumber + 1
            createAndRegisterNewWALFile(newSequenceNumber)
        }
    }


    private fun createAndRegisterNewWALFile(newSequenceNumber: Long): WALFile {
        val newFile = this.directory.file(this.createWalFileName(newSequenceNumber))
        newFile.create()
        val newWALFile = WALFile(newFile, newSequenceNumber)
        this.walFiles.add(newWALFile)
        return newWALFile
    }

    private fun createWalFileName(walSequenceNumber: Long): String {
        return "${WRITE_AHEAD_LOG_FILE_PREFIX}${walSequenceNumber}${WRITE_AHEAD_LOG_FILE_SUFFIX}"
    }

    fun readWalStreaming(walReadBuffer: WALReadBuffer, flushBuffer: () -> Unit) {
        this.state.checkOpen()
        this.lock.read {
            // when we start reading the WAL, we must ignore ALL entries that come BEFORE
            // the first "beginTransaction" command. The reason is that a transaction which
            // was started in the previous WAL file may still be on-going (but the previous
            // WAL file got deleted). Replaying that would mean replaying a partial transaction.
            var firstBeginTransactionReached = false
            for ((index, walFile) in this.walFiles.withIndex()) {
                log.debug { "Replaying file '${walFile}' (${walFile.length.Bytes.toHumanReadableString()}) [${index + 1} / ${this.walFiles.size}]" }
                walFile.withInputStream { input ->
                    input.buffered().use { bufferedInput ->
                        val modifiedStoreIds = mutableSetOf<StoreId>()
                        for (entry in WALEntry.readStreaming(bufferedInput)) {
                            when (entry) {
                                is TransactionCommandEntry -> {
                                    if (!firstBeginTransactionReached) {
                                        // belongs to a partial transaction, ignore
                                        continue
                                    }
                                    walReadBuffer.addOperation(entry.storeId, entry.command)
                                    modifiedStoreIds += entry.storeId
                                }

                                is TransactionCommitEntry -> {
                                    if (!firstBeginTransactionReached) {
                                        // belongs to a partial transaction, ignore
                                        continue
                                    }
                                    walReadBuffer.completeTransaction(modifiedStoreIds, entry.commitTSN)
                                    modifiedStoreIds.clear()
                                }

                                is TransactionStartEntry -> {
                                    // start reading (if we haven't done so already)
                                    firstBeginTransactionReached = true
                                }
                            }
                            if (walReadBuffer.isFull()) {
                                // entry filled the buffer, flush it.
                                flushBuffer()
                            }
                        }
                    }
                }
            }

            // we're at the end of the last WAL file, if there's data
            // left in the buffer, we need to flush it now.
            if (!walReadBuffer.isEmpty()) {
                flushBuffer()
            }
        }
    }


    fun generateChecksumsForCompletedFiles() {
        this.state.checkOpen()
        // we do this outside of any lock to avoid blocking writers;
        // technically we're only computing checksums on immutable files so there's
        // not much that can go wrong. Worst case is that we generate a checksum
        // for a file that's being deleted as we compute the checksum.

        // grab a copy of the WAL files list
        val walFiles = this.lock.read { this.walFiles.toMutableList() }
        if (walFiles.isEmpty()) {
            // we don't have any WAL files yet, nothing to do.
            return
        }

        // just a safeguard: sort the WAL files ascending by min sequence number. This
        // *should* already be the case anyway. Since we're constantly shortening
        // the WAL again, this list should not have thousands of entries, so the
        // overhead for sorting should be negligible.
        walFiles.sortBy { it.sequenceNumber }

        // drop the last file from the list because this is the "active" WAL
        // which is not complete yet; no point in computing a checksum for it.
        walFiles.removeLast()

        // for all other files, we generate checksums.
        for (walFile in walFiles) {
            try {
                walFile.createChecksumFileIfNecessary()
            } catch (e: Exception) {
                log.warn(e) { "Could not create checksum for WAL file '${walFile.file.path}' (reason: ${e})! Will skip this file and continue." }
            }
        }
    }

    private fun getTransactionInfo(file: WALFile, ignoreTruncatedEntries: Boolean): WALTransactionInfo {
        file.inputStream().buffered().use { inputStream ->
            val startedTransactionTSNs = mutableSetOf<TSN>()
            val completedTransactionTSNs = mutableSetOf<TSN>()
            val commandTSNs = mutableSetOf<TSN>()

            var previousTSN = -1L
            for (walEntry in WALEntry.readStreaming(inputStream, ignoreTruncatedEntries)) {
                if (walEntry.commitTSN < previousTSN) {
                    throw WriteAheadLogCorruptedException("Write-Ahead-Log file '${file.file.path}' contains non-ascending transaction serial numbers!")
                }
                previousTSN = walEntry.commitTSN
                when (walEntry) {
                    is TransactionCommandEntry -> commandTSNs += walEntry.commitTSN
                    is TransactionStartEntry -> startedTransactionTSNs += walEntry.commitTSN
                    is TransactionCommitEntry -> completedTransactionTSNs += walEntry.commitTSN
                }
            }

            return WALTransactionInfo(
                startedTSNs = startedTransactionTSNs,
                completedTSNs = completedTransactionTSNs,
                commandTSNs = commandTSNs,
            )
        }
    }

    fun report(): WalReport {
        this.state.checkOpen()
        this.lock.read {
            return WalReport(this.walFiles.map { this.generateWalFileReport(it) })
        }
    }

    private fun generateWalFileReport(walFile: WALFile): WalFileReport {
        return WalFileReport(
            path = walFile.file.path,
            name = walFile.file.name,
            sizeInBytes = walFile.file.length,
        )
    }

    override fun close() {
        this.closeInternal(ManagerState.CLOSED)
    }

    fun closePanic() {
        this.closeInternal(ManagerState.PANIC)
    }

    private fun closeInternal(closeState: ManagerState) {
        if (this.state.isClosed()) {
            this.state = closeState
            return
        }
        this.state = closeState
    }

    private data class WALTransactionInfo(
        val startedTSNs: Set<TSN>,
        val completedTSNs: Set<TSN>,
        val commandTSNs: Set<TSN>,
    ) {

        val allTSNs: Set<TSN> by lazy(LazyThreadSafetyMode.NONE) {
            this.startedTSNs + this.completedTSNs + this.completedTSNs
        }

        val startedButNotCompletedTSNs: Set<TSN> by lazy(LazyThreadSafetyMode.NONE) {
            this.startedTSNs - this.completedTSNs
        }

        val minStartTransactionTSN: TSN? by lazy(LazyThreadSafetyMode.NONE) {
            this.startedTSNs.minOrNull()
        }

        val maxUncompletedTransactionStartTSN: TSN? by lazy(LazyThreadSafetyMode.NONE) {
            this.startedButNotCompletedTSNs.maxOrNull()
        }

        val maxCompletedTSN: TSN? by lazy(LazyThreadSafetyMode.NONE) {
            this.completedTSNs.maxOrNull()
        }
    }

}