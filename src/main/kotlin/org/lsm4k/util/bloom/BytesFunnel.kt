package org.lsm4k.util.bloom

import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.bytes.BytesOutput

@Suppress("UnstableApiUsage", "JavaIoSerializableObjectMustHaveReadResolve")
data object BytesFunnel: Funnel<Bytes> {

    override fun funnel(from: Bytes, into: PrimitiveSink) {
        from.writeToOutput(GuavaPrimitiveSinkAdapter(into))
    }

    private class GuavaPrimitiveSinkAdapter(
        private val sink: PrimitiveSink
    ): BytesOutput {

        override fun write(bytes: ByteArray) {
            sink.putBytes(bytes)
        }

        override fun write(bytes: ByteArray, offset: Int, length: Int) {
            sink.putBytes(bytes, offset, length)
        }

    }

}