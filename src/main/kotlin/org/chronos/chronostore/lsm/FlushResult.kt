package org.chronos.chronostore.lsm

import org.chronos.chronostore.io.vfs.VirtualFile

class FlushResult(
    val targetFile: VirtualFile?,
    val bytesWritten: Long,
    val entriesWritten: Int,
    val runtimeMillis: Long,
) {

    companion object {

        val EMPTY = FlushResult(null, 0, 0, 0)

    }

    val throughputPerSecond: Long
        get() {
            if(this.bytesWritten <= 0){
                return 0L
            }
            if(runtimeMillis <= 0L){
                // we round up the runtime to 1ms, so we would
                // have 1000 times the bytes written per second.
                return bytesWritten * 1000
            }

            return (this.bytesWritten / (runtimeMillis / 1000.0)).toLong()
        }

}