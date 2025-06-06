package org.lsm4k.util.bits

import java.io.InputStream
import java.io.OutputStream

object BitTricks {

    fun OutputStream.writeStableLong(long: Long) {
        this.writeUnsignedLong(long xor Long.MIN_VALUE)
    }

    private fun OutputStream.writeUnsignedLong(long: Long) {
        write((long ushr 56).toByte().toInt())
        write((long ushr 48).toByte().toInt())
        write((long ushr 40).toByte().toInt())
        write((long ushr 32).toByte().toInt())
        write((long ushr 24).toByte().toInt())
        write((long ushr 16).toByte().toInt())
        write((long ushr 8).toByte().toInt())
        write(long.toByte().toInt())
    }

    fun InputStream.readStableLong(): Long {
        return this.readUnsignedLong() xor Long.MIN_VALUE
    }

    private fun InputStream.readUnsignedLong(): Long {
        val c1 = read().toLong()
        val c2 = read().toLong()
        val c3 = read().toLong()
        val c4 = read().toLong()
        val c5 = read().toLong()
        val c6 = read().toLong()
        val c7 = read().toLong()
        val c8 = read().toLong()
        if ((c1 or c2 or c3 or c4 or c5 or c6 or c7 or c8) < 0) {
            throw IllegalStateException("Input stream read() returned a negative value!")
        }
        return (c1 shl 56) or (c2 shl 48) or (c3 shl 40) or (c4 shl 32) or
            (c5 shl 24) or (c6 shl 16) or (c7 shl 8) or c8
    }

    fun OutputStream.writeStableInt(int: Int) {
        this.writeUnsignedInt(int xor Int.MIN_VALUE)
    }

    private fun OutputStream.writeUnsignedInt(int: Int) {
        write((int ushr 24).toByte().toInt())
        write((int ushr 16).toByte().toInt())
        write((int ushr 8).toByte().toInt())
        write(int.toByte().toInt())
    }

    fun InputStream.readStableInt(): Int {
        return this.readUnsignedInt() xor Int.MIN_VALUE
    }

    private fun InputStream.readUnsignedInt(): Int {
        val c1 = read()
        val c2 = read()
        val c3 = read()
        val c4 = read()
        if ((c1 or c2 or c3 or c4) < 0) {
            throw IllegalStateException("Input stream read() returned a negative value!")
        }
        return (c1 shl 24) or (c2 shl 16) or (c3 shl 8) or c4
    }
}
