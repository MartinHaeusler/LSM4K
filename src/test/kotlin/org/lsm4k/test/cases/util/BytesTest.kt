package org.lsm4k.test.cases.util

import org.junit.jupiter.api.Test
import org.lsm4k.util.bytes.BasicBytes
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo

class BytesTest {

    @Test
    fun canPerformHashCodeAndEquals(){
        val bytesA = BasicBytes(byteArrayOf(12,13,34,56))
        val bytesB = BasicBytes(byteArrayOf(16,32))
        val bytesA1 = BasicBytes(byteArrayOf(12,13,34,56))

        expectThat(bytesA){
            isEqualTo(bytesA)
            isEqualTo(bytesA1)
            isNotEqualTo(bytesB)

            get { hashCode() }.and {
                isEqualTo(bytesA.hashCode())
                isEqualTo(bytesA1.hashCode())
                isNotEqualTo(bytesB.hashCode())
            }
        }
    }

}