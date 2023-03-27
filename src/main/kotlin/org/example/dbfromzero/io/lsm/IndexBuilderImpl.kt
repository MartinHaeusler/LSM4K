package org.example.dbfromzero.io.lsm

import org.example.dbfromzero.util.Bytes
import org.example.dbfromzero.util.LittleEndianUtil

class IndexBuilderImpl(
    private val indexStorage: KeyValueFileWriter,
    private val indexRate: Int
) : IndexBuilder {

    private var keyCount: Int = 0

    init {
        require(indexRate > 0) { "Index rate must be positive, but got: $indexRate!" }
    }

    override fun accept(position: Long, key: Bytes) {
        if (keyCount % indexRate == 0) {
            indexStorage.append(key, Bytes(LittleEndianUtil.writeLittleEndianLong(position)))
        }
        keyCount++
    }

}