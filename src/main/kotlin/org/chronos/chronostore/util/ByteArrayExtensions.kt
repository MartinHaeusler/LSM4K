package org.chronos.chronostore.util

import java.util.*

object ByteArrayExtensions {

    fun ByteArray.hex(): String {
        return HexFormat.of().formatHex(this)
    }

}