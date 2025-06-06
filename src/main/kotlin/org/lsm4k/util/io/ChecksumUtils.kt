package org.lsm4k.util.io

import com.google.common.hash.HashCode
import com.google.common.hash.HashFunction
import com.google.common.io.ByteStreams
import org.lsm4k.io.vfs.VirtualFile
import org.lsm4k.util.StreamExtensions.checked
import org.lsm4k.util.StreamExtensions.digest
import java.security.MessageDigest
import java.util.zip.CRC32
import java.util.zip.Checksum

object ChecksumUtils {

    fun VirtualFile.computeChecksum(checksum: Checksum): Long {
        // note that we provide the checksum to the CheckedInputStream. Anything we
        // pull from that input stream will update the checksum. Exhausting the
        // input stream will take care that every byte is read.
        createInputStream().buffered().checked(checksum).use { input ->
            ByteStreams.exhaust(input)
        }
        return checksum.value
    }

    fun VirtualFile.computeCrc32(): Long {
        return this.computeChecksum(CRC32())
    }

    fun VirtualFile.computeDigest(digest: MessageDigest): ByteArray {
        // note that we provide the digest to the DigestInputStream. Anything we
        // pull from that input stream will update the checksum. Exhausting the
        // input stream will take care that every byte is read.
        createInputStream().buffered().digest(digest).use { input ->
            ByteStreams.exhaust(input)
        }
        return digest.digest()
    }

    fun VirtualFile.computeMD5(): ByteArray {
        return this.computeDigest(MessageDigest.getInstance("MD5"))
    }

    fun VirtualFile.computeHash(hashFunction: HashFunction): HashCode {
        return createInputStream().use { input ->
            val processor = HashByteProcessor(hashFunction.newHasher())
            ByteStreams.readBytes(input, processor)
        }
    }


}