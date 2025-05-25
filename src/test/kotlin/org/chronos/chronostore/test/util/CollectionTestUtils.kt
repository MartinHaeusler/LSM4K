package org.chronos.chronostore.test.util

import org.chronos.chronostore.util.bytes.Bytes
import java.nio.charset.Charset

object CollectionTestUtils {

    fun List<Pair<Bytes, Bytes>>.asStrings(charset: Charset = Charsets.UTF_8): List<Pair<String, String>> {
        return this.map { it.first.asString(charset) to it.second.asString(charset) }
    }

}