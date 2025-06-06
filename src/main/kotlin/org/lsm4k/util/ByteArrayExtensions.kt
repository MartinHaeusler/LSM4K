package org.lsm4k.util

import java.util.*
import kotlin.math.min

object ByteArrayExtensions {

    fun ByteArray.hex(maxLength: Int = Int.MAX_VALUE): String {
        return HexFormat.of().formatHex(this, 0, min(this.size, maxLength))
    }

}