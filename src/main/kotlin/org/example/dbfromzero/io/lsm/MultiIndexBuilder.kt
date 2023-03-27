package org.example.dbfromzero.io.lsm

import org.example.dbfromzero.util.Bytes

class MultiIndexBuilder : IndexBuilder {


    private val indexBuilders: List<IndexBuilder>

    constructor(indexBuilders: Iterable<IndexBuilder>) {
        this.indexBuilders = indexBuilders.toList()
    }

    override fun accept(position: Long, key: Bytes) {
        for (indexBuilder in indexBuilders) {
            indexBuilder.accept(position, key)
        }
    }


}