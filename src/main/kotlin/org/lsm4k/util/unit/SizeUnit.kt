package org.lsm4k.util.unit

import org.lsm4k.impl.annotations.PersistentClass

@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in BinarySize objects.")
enum class SizeUnit {

    BYTE,

    KILOBYTE,

    MEBIBYTE,

    GIBIBYTE,

    TEBIBYTE,

    ;

    fun toBytes(value: Long): Long {
        return when (this) {
            BYTE -> value
            KILOBYTE -> value * 1024
            MEBIBYTE -> value * 1024 * 1024
            GIBIBYTE -> value * 1024 * 1024 * 1024
            TEBIBYTE -> value * 1024 * 1024 * 1024 * 1024
        }

    }

}