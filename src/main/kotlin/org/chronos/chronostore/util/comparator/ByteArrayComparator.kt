package org.chronos.chronostore.util.comparator

interface ByteArrayComparator : Comparator<ByteArray> {

    fun compare(
        left: ByteArray,
        leftFromInclusive: Int,
        leftToInclusive: Int,
        right: ByteArray,
        rightFromInclusive: Int,
        rightToInclusive: Int,
    ): Int

}