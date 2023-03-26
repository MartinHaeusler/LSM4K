package org.example.dbfromzero.io.lsm

import org.example.dbfromzero.io.vfs.VirtualFile
import org.example.dbfromzero.util.Bytes
import org.example.dbfromzero.util.LittleEndianExtensions.readLittleEndianInt
import org.example.dbfromzero.util.PrefixIO
import java.io.EOFException
import java.io.InputStream

/**
 * A stream-based linear reader for a file containing prefix-length-encoded key-value pairs.
 *
 * The format is:
 *
 * ```
 * [length of key 1][key 1 content bytes][length of value 1][value 1 content bytes]
 * [length of key 2][key 2 content bytes][length of value 2][value 2 content bytes]
 * [length of key 3][key 3 content bytes][length of value 3][value 3 content bytes]
 * ...
 * ```
 *
 * The `[length of key]` and `[length of value]` fields are always little-endian-encoded integers (32 bit, fixed length).
 */
class KeyValueFileReader(
    private val inputStream: InputStream
) : AutoCloseable {

    constructor(virtualFile: VirtualFile) : this(virtualFile.createInputStream())

    @Transient
    private var isOpen = true

    private var hasReadKey = false

    fun readKey(): Bytes? {
        assertIsOpen()
        check(!hasReadKey) { "This reader is currently expecting a call to readValue() or skipValue()!" }
        return try {
            val key = PrefixIO.readBytes(this.inputStream)
            hasReadKey = true
            key
        } catch (e: EOFException) {
            null
        }
    }

    fun readValue(): Bytes {
        assertIsOpen()
        check(hasReadKey) { "This reader is currently expecting a call to readKey()!" }
        val value = PrefixIO.readBytes(this.inputStream)
        hasReadKey = false
        return value
    }


    fun skipValue() {
        assertIsOpen()
        check(hasReadKey) { "This reader is currently expecting a call to readKey()!" }
        val valueLength = inputStream.readLittleEndianInt()
        this.skipBytes(valueLength.toLong())
        hasReadKey = false
    }

    fun readKeyValuePair(): Pair<Bytes, Bytes>? {
        val key = this.readKey()
            ?: return null // end of file
        val value = this.readValue();
        return Pair(key, value)
    }

    fun readKeySkipValue(): Bytes? {
        assertIsOpen()
        val key = this.readKey()
            ?: return null
        this.skipValue()
        return key
    }

    fun skipBytes(length: Long) {
        var remainingToSkip = length
        while (remainingToSkip > 0) {
            val skipped = inputStream.skip(remainingToSkip)
            if (skipped == 0L) {
                throw RuntimeException("Failed to skip $length" + " only skipped " + (length - remainingToSkip))
            }
            remainingToSkip -= skipped
        }
    }


    override fun close() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        this.inputStream.close()
    }

    private fun assertIsOpen() {
        check(this.isOpen) { "This reader has already been closed!" }
    }
}