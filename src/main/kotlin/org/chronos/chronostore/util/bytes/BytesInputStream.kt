package org.chronos.chronostore.util.bytes

import java.io.InputStream

class BytesInputStream(
    private val iterator: Iterator<Byte>
) : InputStream() {

    override fun read(): Int {
        if (!this.iterator.hasNext()) {
            return -1
        }
        return this.iterator.next().toInt()
    }


}