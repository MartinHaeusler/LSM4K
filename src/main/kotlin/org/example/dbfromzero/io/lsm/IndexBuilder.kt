package org.example.dbfromzero.io.lsm

import org.chronos.chronostore.util.Bytes

interface IndexBuilder {

    companion object {

        fun create(storage: KeyValueFileWriter, indexRate: Int): IndexBuilder {
            return IndexBuilderImpl(storage, indexRate)
        }

        fun create(indexBuilders: Iterable<IndexBuilder>): IndexBuilder {
            return MultiIndexBuilder(indexBuilders)
        }

    }

    fun accept(position: Long, key: Bytes)


}