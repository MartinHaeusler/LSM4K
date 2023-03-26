package org.example.dbfromzero.test.cases.util

import org.example.dbfromzero.util.Bytes
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo

class BytesTest {

    @Test
    fun canPerformHashCodeAndEquals(){
        val bytesA = Bytes(byteArrayOf(12,13,34,56))
        val bytesB = Bytes(byteArrayOf(16,32))
        val bytesA1 = Bytes(byteArrayOf(12,13,34,56))

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