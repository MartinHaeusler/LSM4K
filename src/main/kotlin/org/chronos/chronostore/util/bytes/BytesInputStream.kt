package org.chronos.chronostore.util.bytes

import java.io.InputStream

class BytesInputStream(
    private val iterator: Iterator<Byte>
) : InputStream() {

    override fun read(): Int {
        if (!this.iterator.hasNext()) {
            return -1
        }
        // to convert negative byte values to positive
        // integers, we perform the "and 0xff". If we
        // don't do this, we get negative integers, which
        // is not what we want.
        return this.iterator.next().toInt() and 0xff
    }


}