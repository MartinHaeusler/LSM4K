package org.chronos.chronostore.io.format.datablock

import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import java.io.InputStream

object DataBlockUtil {

    @JvmStatic
    fun seekInData(inputStream: InputStream, key: KeyAndTimestamp): Pair<Command, Boolean>? {
        var previous: Command? = null
        while (inputStream.available() > 0) {
            val command = Command.readFromStream(inputStream)
            val cmp = command.keyAndTimestamp.compareTo(key)
            if (cmp == 0) {
                // exact match (unlikely, but possible)
                return Pair(command, false)
            } else if (cmp > 0) {
                // this key is bigger, use the previous one
                return if (previous?.key == key.key) {
                    Pair(previous, false)
                } else {
                    null
                }
            } else {
                // remember this one, but keep searching
                previous = command
            }
        }
        // we're at the end of the block, maybe the last
        // entry matches?
        return if (previous?.key == key.key) {
            Pair(previous, true)
        } else {
            null
        }
    }

}