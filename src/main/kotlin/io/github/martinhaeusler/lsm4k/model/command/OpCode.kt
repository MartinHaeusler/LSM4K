package io.github.martinhaeusler.lsm4k.model.command

/**
 * The [OpCode] defines the semantics of a [Command].
 */
enum class OpCode(val byte: Byte) {

    /** The [Command] is a "put", i.e. an insertion or an update. */
    PUT(0b00000001),

    /** The [Command] is a deletion. */
    DEL(0b00000010);

    companion object {

        fun fromByte(byte: Int): OpCode {
            return fromByte(byte.toByte())
        }

        fun fromByte(byte: Byte): OpCode {
            return when (byte) {
                PUT.byte -> PUT
                DEL.byte -> DEL
                else -> throw IllegalArgumentException("Unknown OpCode: $byte")
            }
        }

    }

}