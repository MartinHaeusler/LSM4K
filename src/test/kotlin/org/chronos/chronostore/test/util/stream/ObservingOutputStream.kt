package org.chronos.chronostore.test.util.stream

import java.io.OutputStream

class ObservingOutputStream(
    private val out: OutputStream
): OutputStream() {

    companion object {

        fun OutputStream.observe(): OutputStream {
            return ObservingOutputStream(this)
        }

    }

    override fun write(b: Int) {
        println("OUT: " + Integer.toBinaryString(b.and(0xFF)).padStart(8, '0'))
        this.out.write(b)
    }

}