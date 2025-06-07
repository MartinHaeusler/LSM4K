package io.github.martinhaeusler.lsm4k.benchmark.comparative.xodus

import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import jetbrains.exodus.ByteIterable

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