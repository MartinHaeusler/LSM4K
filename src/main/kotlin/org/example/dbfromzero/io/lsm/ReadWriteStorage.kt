package org.example.dbfromzero.io.lsm

import org.example.dbfromzero.util.Bytes

interface ReadWriteStorage: AutoCloseable {

    val size: Int

    fun put(key: Bytes, value: Bytes)

    fun get(key: Bytes): Bytes?

    fun delete(key: Bytes): Boolean

}