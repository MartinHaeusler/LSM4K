package io.github.martinhaeusler.lsm4k.lsm

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.util.FileIndex

class FlushResult(
    val targetFile: VirtualFile,
    val targetFileIndex: FileIndex,
    val bytesWritten: Long,
    val entriesWritten: Int,
    val runtimeMillis: Long,
) {

    val throughputPerSecond: Long
        get() {
            if (this.bytesWritten <= 0) {
                return 0L
            }
            if (runtimeMillis <= 0L) {
                // we round up the runtime to 1ms, so we would
                // have 1000 times the bytes written per second.
                return bytesWritten * 1000
            }

            return (this.bytesWritten / (runtimeMillis / 1000.0)).toLong()
        }

}