package org.chronos.chronostore.lsm

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Keeps track of the garbage files produced by an LSM tree which need to be deleted eventually.
 *
 * When LSM tree files are compacted (i.e. merged together), the old files sometimes need to
 * remain in place because one or more cursors are currently iterating over them. These old files can
 * be dropped as soon as the last cursor which operated on them has been closed, but not
 * earlier. Since we do not want to put a hard limit on the maximum runtime of a cursor,
 * we have to assume that the system crashes or is rebooted before we have a chance to delete
 * the garbage files. We still keep the data structure only in-memory. At startup, we check
 * which files exist on disk and which files *should* exist according to the manifest, and
 * any excess files (of known types) get deleted immediately. Therefore, the garbage manager
 * starts up "empty" every time.
 */
class GarbageFileManager {

    private val lock = ReentrantReadWriteLock(true)
    private val _garbageFiles = mutableSetOf<String>()

    fun addAll(filesToMerge: List<String>) {
        this.lock.write {
            this._garbageFiles += filesToMerge
        }
    }

    fun removeAll(deleted: Iterable<String>) {
        this.lock.write {
            this._garbageFiles -= deleted.toSet()
        }
    }

    var garbageFiles: Set<String>
        get() {
            this.lock.read {
                // the caller will want a "point in time" snapshot
                return _garbageFiles.toSet()
            }
        }
        set(garbageFileNames) {
            this.lock.write {
                this._garbageFiles.clear()
                this._garbageFiles += garbageFileNames
            }
        }


}