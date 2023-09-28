package org.chronos.chronostore.util

import java.util.*

object ByteArrayExtensions {

    fun ByteArray.hex(maxLength: Int = Int.MAX_VALUE): String {
        return HexFormat.of().formatHex(this, 0, maxLength)
    }

}