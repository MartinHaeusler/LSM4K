package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.Bytes

object ChronoStoreFileFormat {

    val FILE_MAGIC_BYTES = Bytes(
        byteArrayOf(
            0b01100011, // c
            0b01101000, // h
            0b01110010, // r
            0b01101111, // o
            0b01101110, // n
            0b01101111, // o
            0b01110011, // s
            0b00000000, // <null>
        )
    )

    val BLOCK_MAGIC_BYTES = Bytes(
        byteArrayOf(
            0b01100010, // b
            0b01101100, // l
            0b01101111, // o
            0b01100011, // c
            0b01101011, // k
            0b01100010, // b
            0b01100111, // g
            0b01101110, // n
        )               // blockbgn = block begin
    )

    enum class Version(
        val versionString: String
    ) {

        ONE_0_0("1.0.0")

        ;

        companion object {

            fun fromString(string: String): Version {
                val trim = string.trim()
                for (literal in values()) {
                    if (literal.versionString.equals(string, ignoreCase = true)) {
                        return literal
                    }
                }
                throw IllegalArgumentException("Unknown ChronoStore file format version: '${string}'!")
            }

        }

        override fun toString(): String {
            return this.versionString
        }

    }

}