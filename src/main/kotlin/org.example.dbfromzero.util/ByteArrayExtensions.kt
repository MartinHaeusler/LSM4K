package org.example.dbfromzero.util

import java.util.*

object ByteArrayExtensions {

    fun ByteArray.hex(): String {
        return HexFormat.of().formatHex(this)
    }

}