package org.example.dbfromzero.test.cases.io.lsm

import java.util.*

enum class PutDeleteGet(val putRate: Double, val deleteRate: Double) {
    PUT_HEAVY(0.9, 0.05), GET_HEAVY(0.1, 0.05), DELETE_HEAVY(0.45, 0.45), BALANCED(0.33, 0.33);

    val getRate: Double = 1.0 - putRate - deleteRate

    fun select(r: Double): Operation {
        var r = r
        if (r < putRate) {
            return Operation.PUT
        }
        r -= putRate
        return if (r < deleteRate) Operation.DELETE else Operation.GET
    }

    fun select(random: Random): Operation {
        return select(random.nextDouble())
    }

}