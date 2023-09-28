package org.chronos.chronostore.util.bits

object ByteSizeUtil {

    fun humanReadableByteCountBin(bytes: Int): String {
        return this.humanReadableByteCountBin(bytes.toLong())
    }

    fun humanReadableByteCountBin(bytes: Long): String {
        return when {
            bytes == Long.MIN_VALUE || bytes < 0 -> "N/A"
            bytes < 1024L -> "$bytes B"
            bytes <= 0xfffccccccccccccL shr 40 -> "%.1f KiB".format(bytes.toDouble() / (0x1 shl 10))
            bytes <= 0xfffccccccccccccL shr 30 -> "%.1f MiB".format(bytes.toDouble() / (0x1 shl 20))
            bytes <= 0xfffccccccccccccL shr 20 -> "%.1f GiB".format(bytes.toDouble() / (0x1 shl 30))
            bytes <= 0xfffccccccccccccL shr 10 -> "%.1f TiB".format(bytes.toDouble() / (0x1 shl 40))
            bytes <= 0xfffccccccccccccL -> "%.1f PiB".format((bytes shr 10).toDouble() / (0x1 shl 40))
            else -> "%.1f EiB".format((bytes shr 20).toDouble() / (0x1 shl 40))
        }
    }

}