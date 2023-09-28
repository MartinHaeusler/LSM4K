package org.chronos.chronostore.lsm

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream
import java.lang.IllegalStateException
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Keeps track of the garbage files produced by an LSM tree which need to be deleted eventually.
 *
 * When LSM tree files are compacted (i.e. merged together), the old files sometimes need to
 * remain in place because a transaction is currently iterating over them. These old files can
 * be dropped as soon as the last transaction which operated on them has been closed, but not
 * earlier. Since we do not want to put a hard limit on the maximum runtime of a transaction,
 * we have to assume that the system crashes or is rebooted before we have a chance to delete
 * the garbage files; the conclusion is that we have to track (in a persistent fashion) which
 * files we want to delete.
 *
 * This class maintains a special "garbage.log" file which is overwritten atomically and always
 * contains the list of files we want to delete. Additionally, these files are kept in-memory
 * for quick access (which is only an optimization; it's the content of the file that matters).
 */
class GarbageFileManager(
    val file: VirtualReadWriteFile
) {

    companion object {

        const val FILE_NAME = "garbage.log"
        const val END_OF_FILE = "-- END OF FILE : d7ba411a-510a-4c79-a2a9-8f6bdda37fd3 : END OF FILE --"
    }

    private val lock = ReentrantReadWriteLock(true)

    private val _garbageFiles = mutableListOf<String>()

    init {
        // read garbage files
        this._garbageFiles += readGarbageFiles()
    }

    private fun readGarbageFiles(): List<String> {
        if (!this.file.exists()) {
            // we have no garbage file, nothing to do
            return emptyList()
        }
        this.file.withInputStream { inputStream ->
            inputStream.bufferedReader().use { reader ->
                val lines = reader.readLines()
                if (lines.lastOrNull() != END_OF_FILE) {
                    throw IllegalStateException("Expected file '${this.file.path}' to end with the garbage file tombstone, but it didn't! File is likely corrupted.")
                }
                return lines.dropLast(1)
            }
        }
    }

    fun addAll(filesToMerge: List<String>) {
        this.lock.write {
            this.garbageFiles = (filesToMerge + this._garbageFiles).distinct()
        }
    }

    fun removeAll(deleted: Set<String>) {
        this.lock.write {
            this._garbageFiles.removeAll(deleted)
        }
    }

    var garbageFiles: List<String>
        get() {
            this.lock.read {
                // the caller will want a "point in time" snapshot
                return _garbageFiles.toList()
            }
        }
        set(garbageFileNames) {
            val garbageFileContainingWhitespace = garbageFileNames.firstOrNull { it.any(Char::isWhitespace) }
            require(garbageFileContainingWhitespace == null) {
                "Cannot store garbage files: argument file '${garbageFileContainingWhitespace}' contains whitespace characters!"
            }

            this.lock.write {
                this.file.deleteOverWriterFileIfExists()
                this.file.createOverWriter().use { overWriter ->
                    overWriter.outputStream.bufferedWriter().use { writer ->
                        for (fileName in garbageFileNames) {
                            writer.write(fileName)
                            writer.newLine()
                        }
                        // write the end-of-file "tombstone". This will help
                        // us later to identify if the file has only been
                        // partially written to disk. This should actually
                        // never happen because of the way the overWriter works,
                        // but better be safe than sorry.
                        writer.write(END_OF_FILE)
                        writer.flush()
                    }
                    overWriter.commit()
                }
                // make a safety copy to prevent modification from outside this class
                _garbageFiles.clear()
                _garbageFiles.addAll(garbageFileNames)
            }
        }


}