package org.lsm4k.benchmark.comparative.xodus

import jetbrains.exodus.ByteIterable
import org.lsm4k.util.bytes.BasicBytes
import org.lsm4k.util.bytes.Bytes

object XodusUtils {

    fun ByteIterable.toBytes(): Bytes {
        val byteArray = this.bytesUnsafe
        val arr = if(this.length < byteArray.size){
            byteArray.sliceArray(0..<this.length)
        }else{
            byteArray
        }
        return BasicBytes(byteArray)
    }

}