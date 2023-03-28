package org.example.dbfromzero.test.cases.io.lsm

import java.util.*

enum class RandomSeed(private val seed: Long) {

    CAFE(0xCAFE),

    DEADBEEF(0xDEADBEEF),

    BAD(0xBAD);

    fun random(): Random {
        return Random(seed)
    }
}