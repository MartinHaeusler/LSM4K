package org.example.dbfromzero.io.lsm

import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.Bytes
import java.util.*

/**
 * Supports finding a specific key within an SSTable file.
 *
 * Efficiency of the lookups is achieved with an [index] containing the positions of selected entries,
 * which are stored separately.
 *
 * In contrast to [KeyValueFileReader], this reader is **not** stream-based and has no "current position".
 */
class RandomAccessKeyValueFileReader(
    val virtualFile: VirtualFile,
    val index: TreeMap<Bytes, Long>,
) {

    companion object {

        fun readIndexAndClose(indexFile: VirtualFile): TreeMap<Bytes, Long> {
            return readIndexAndClose(KeyValueFileIterator(KeyValueFileReader(indexFile)))
        }

        fun readIndexAndClose(indexFileIterator: KeyValueFileIterator): TreeMap<Bytes, Long> {
            indexFileIterator.use { iterator ->
                val index = TreeMap<Bytes, Long>()
                for (pair in iterator) {
                    // index file values are always little-endian long values.
                    index[pair.first] = pair.second.readLittleEndianLong()
                }
                return index
            }
        }

    }

    private fun findKeyInReader(key: Bytes, reader: KeyValueFileReader): Bytes? {
        reader.skipBytes(computeSearchStartIndex(key))
        return scanForKey(key, reader)
    }

    private fun computeSearchStartIndex(key: Bytes): Long {
        return this.index.floorEntry(key)?.value ?: 0L
    }

    private fun scanForKey(key: Bytes, reader: KeyValueFileReader): Bytes? {
        while (true) {
            val currentKey = reader.readKey()
                ?: return null // end of file
            val cmp = key.compareTo(currentKey)
            when {
                cmp == 0 -> return reader.readValue() // hit -> return the value
                cmp < 0 -> reader.skipValue() // key is too small -> keep searching
                else -> return null // current key is bigger -> key not present
            }
        }
    }

    fun get(key: Bytes): Bytes? {
        // specifically don't use buffered IO cause we hopefully won't have to read much
        // and we also get to use KeyValueFileReader.skipValue to avoid large segments of data
        // might be worth benchmarking the effect of buffered IO?
        KeyValueFileReader(this.virtualFile.createInputStream()).use { reader ->
            return findKeyInReader(key, reader)
        }
    }
}