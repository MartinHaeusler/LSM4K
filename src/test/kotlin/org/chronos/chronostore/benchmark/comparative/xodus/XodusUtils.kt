package org.chronos.chronostore.benchmark.comparative.xodus

import jetbrains.exodus.ByteIterable
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes

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