package io.github.martinhaeusler.lsm4k.manifest

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import io.github.martinhaeusler.lsm4k.api.exceptions.ManifestException
import io.github.martinhaeusler.lsm4k.api.exceptions.StoreAlreadyExistsException
import io.github.martinhaeusler.lsm4k.api.exceptions.StoreNotFoundException
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import io.github.martinhaeusler.lsm4k.manifest.ManifestUtils.validateManifestSequenceNumber
import io.github.martinhaeusler.lsm4k.manifest.operations.CheckpointOperation
import io.github.martinhaeusler.lsm4k.manifest.operations.CreateStoreOperation
import io.github.martinhaeusler.lsm4k.manifest.operations.FlushOperation
import io.github.martinhaeusler.lsm4k.manifest.operations.ManifestOperation
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.IOExtensions.withInputStream
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.StringExtensions.toSingleLine
import io.github.martinhaeusler.lsm4k.util.json.JsonUtil
import java.io.InputStream
import java.io.Writer
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write
import kotlin.time.measureTimedValue

/**
 * This class handles the I/O operations and the locking for the [Manifest].
 *
 * It allows to easily access the manifest via [getManifest], appending operations
 * to it via [appendOperation], and to create checkpoints via [createCheckpoint] (unconditionally) or
 * [createCheckpointIfNumberOfOperationsExceeds] (based on minimum operation count).
 *
 * Since this class handles all required locking operations, it is thread-safe. For any [io.github.martinhaeusler.lsm4k.api.DatabaseEngine]
 * instance, there should be exactly one [ManifestFile] instance (which can manage and refer to multiple actual files on disk).
 */
class ManifestFile(
    val file: VirtualReadWriteFile,
) {

    companion object {

        /**
         * The hash function used on each particular entry of the WAL.
         *
         * ATTENTION: DO NOT CHANGE THIS! Changing this will make all WAL files out there permanently INVALID!
         */
        val HASH_FUNCTION: HashFunction = Hashing.murmur3_128()

        const val FILE_NAME = "manifest.lsmf"

    }

    private var manifestCache: Manifest? = null
    private var operationCount = 0

    private var replayLock: Lock = ReentrantLock(true)
    private var readWriteLock = ReentrantReadWriteLock(true)

    private fun readManifestFromFile(): Manifest {
        val timedValue = measureTimedValue {
            this.replayLock.withLock {
                // always fsync manifest files before reading them. It could happen that:
                // - we write to a manifest file (but we don't reach the sync point)
                // - the process (not the OS) gets killed half-way
                // - the process gets restarted on the same directory
                // - the process now reads the half-written file FROM THE OS CACHE
                // - the process acts upon the data
                // - power goes out
                // -> We've read from a manifest which is no longer there :BOOM:
                //
                // Solution: play it safe, fsync the file before reading it.
                this.file.fsync()
                val (replayedManifest, operationCount) = this.file.withInputStream(this::parseManifestFromInputStream)
                this.operationCount = operationCount
                replayedManifest
            }
        }

        return timedValue.value
    }

    private fun parseManifestFromInputStream(inputStream: InputStream): Pair<Manifest, Int> {
        return inputStream.bufferedReader().use { reader ->
            val operationsSequence = reader.lineSequence()
                .mapIndexed { index, line ->
                    if (line.isBlank()) {
                        null
                    } else {
                        try {
                            // format: [operation hash][space][operation JSON]
                            val hash = line.substringBefore(' ', missingDelimiterValue = "").toInt()
                            val json = line.substringAfter(' ', missingDelimiterValue = "")
                            // check the hash
                            val rehash = HASH_FUNCTION.hashString(json, Charsets.UTF_8).asInt()
                            if (hash != rehash) {
                                throw ManifestException("The operation fingerprint doesn't match; the file has likely been corrupted!")
                            }
                            JsonUtil.readJsonAsObject<ManifestOperation>(json)
                        } catch (e: Exception) {
                            throw ManifestException("Could not read manifest at line ${index + 1}: ${e}", e)
                        }
                    }
                }
                .filterNotNull()

            Manifest.replay(operationsSequence)
        }
    }

    /**
     * Returns the current manifest.
     *
     * @return The current manifest.
     */
    fun getManifest(): Manifest {
        this.readWriteLock.read {
            val cachedManifest = this.manifestCache
            if (cachedManifest != null) {
                return cachedManifest
            }
            val manifest = if (!this.file.exists()) {
                Manifest()
            } else {
                this.readManifestFromFile()
            }
            this.manifestCache = manifest
            return manifest
        }
    }

    fun getOperationCount(): Int {
        this.readWriteLock.read {
            // ensure that the manifest is loaded.
            // This also loads the operation count as a side effect if it wasn't initialized yet.
            this.getManifest()
            return this.operationCount
        }
    }

    /**
     * Appends an operation to the manifest.
     *
     * @param createOperation A lambda that takes the sequence number for the new operation and creates the operation from it.
     *
     * @return The new manifest.
     *
     * @throws ManifestException if the newly created operation is incompatible with the current manifest state.
     */
    fun appendOperation(createOperation: (Int) -> ManifestOperation): Manifest {
        this.readWriteLock.write {
            val manifest = this.getManifest()
            val operation = createOperation(manifest.lastAppliedOperationSequenceNumber + 1)
            validateManifestSequenceNumber(manifest, operation)

            this.file.append { outputStream ->
                outputStream.writer().use { writer ->
                    writer.writeOperation(operation)
                }
            }

            val newManifest = operation.applyToManifest(manifest)
            this.manifestCache = newManifest
            this.operationCount++

            return newManifest
        }
    }

    /**
     * Creates a new store in the manifest.
     *
     * Internally, this appends a single [CreateStoreOperation] to the manifest. Before that, we atomically check
     * if a store with the given [StoreMetadata.storeId] already exists.
     *
     * @param metadata The metadata for the new store to create.
     *
     * @return The new manifest
     *
     * @throws StoreAlreadyExistsException if the given metadata refers to an already existing store.
     */
    fun appendCreateStoreOperation(metadata: StoreMetadata): Manifest {
        this.readWriteLock.write {
            val existingStore = this.getManifest().getStoreOrNull(metadata.storeId)
            if (existingStore != null) {
                throw StoreAlreadyExistsException("Cannot create store '${metadata.storeId}' because it already exists!")
            }
            return this.appendOperation { sequenceNumber ->
                CreateStoreOperation(
                    sequenceNumber = sequenceNumber,
                    storeMetadata = metadata,
                )
            }
        }
    }

    /**
     * Appends a [FlushOperation] to the manifest.
     *
     * @param storeId The store which got flushed.
     * @param flushTargetFileIndex The index of the file which has received the data from the flush operation. Must not be negative.
     *
     * @return the new manifest
     *
     * @throws StoreNotFoundException if there's no store with the given [storeId].
     */
    fun appendFlushOperation(storeId: StoreId, flushTargetFileIndex: FileIndex): Manifest {
        require(flushTargetFileIndex >= 0) {
            "Argument 'flushTargetFileIndex' (${flushTargetFileIndex}) must not be negative!"
        }
        this.readWriteLock.write {
            this.getManifest().getStoreOrNull(storeId)
                ?: throw StoreNotFoundException("Cannot flush store '${storeId}' because it doesn't exist!")
            return this.appendOperation { sequenceNumber ->
                FlushOperation(
                    sequenceNumber = sequenceNumber,
                    storeId = storeId,
                    fileIndex = flushTargetFileIndex,
                )
            }
        }
    }

    /**
     *  Creates a new checkpoint on the manifest if the current [operation count][getOperationCount] is greater than the given [minOperations].
     *
     *  This collapses all manifest operations into a single [CheckpointOperation].
     *
     *  When executed periodically, this can help to reduce the overall size of the
     *  manifest. However, this operation is quite costly so use with care.
     *
     * @param minOperations  The minimum number of required operations that make a checkpoint "worthwhile". If the operation count is
     *                       less than or equal to this value, this method does nothing and returns the current manifest immediately.
     *                       Otherwise, the manifest is compacted by creating a checkpoint. Using 0 consequently always forces a checkpoint.
     *
     *  @return The manifest
     */
    fun createCheckpointIfNumberOfOperationsExceeds(minOperations: Int): Manifest {
        require(minOperations >= 0) { "Precondition violation - argument 'minOperations' must not be negative!" }
        this.readWriteLock.write {
            if (this.getOperationCount() <= minOperations) {
                return this.getManifest()
            }
            return this.createCheckpoint()
        }
    }

    /**
     *  Creates a new checkpoint on the manifest.
     *
     *  This collapses all manifest operations into a single [CheckpointOperation].
     *
     *  When executed periodically, this can help to reduce the overall size of the
     *  manifest. However, this operation is quite costly so use with care.
     *
     *  @return The manifest
     */
    private fun createCheckpoint(): Manifest {
        this.readWriteLock.write {
            val manifest = this.getManifest()
            val checkpoint = CheckpointOperation(
                sequenceNumber = manifest.lastAppliedOperationSequenceNumber + 1,
                wallClockTime = System.currentTimeMillis(),
                checkpoint = manifest,
            )

            this.file.deleteOverWriterFileIfExists()
            this.file.withOverWriter { overWriter ->
                overWriter.outputStream.bufferedWriter().use { writer ->
                    writer.writeOperation(checkpoint)
                }
                overWriter.commit()
            }

            val newManifest = manifest.copy(lastAppliedOperationSequenceNumber = manifest.lastAppliedOperationSequenceNumber + 1)
            this.manifestCache = newManifest
            // our file now only contains one operation, reset the count
            this.operationCount = 1

            return newManifest
        }
    }

    private fun Writer.writeOperation(operation: ManifestOperation) {
        // when appending, make sure that the text resulting from the JSON does not contain newlines,
        // otherwise it will break the (lazy) line-by-line parser!
        val json = JsonUtil.writeJson(operation).toSingleLine()
        val hash = HASH_FUNCTION.hashString(json, Charsets.UTF_8).asInt()
        // format: [operation hash][space][operation JSON]
        write("${hash} ${json}\n")
    }

}