package org.lsm4k.test.util.fakestoredsl

class AutoIndex(
    private var state: Int = 0,
) {

    fun getNextFreeIndex(): Int {
        return this.state++
    }

    fun useFixedIndex(value: Int): Int {
        if (state > value) {
            throw IllegalStateException(
                "Cannot use manual index ${value}: the auto-index ${state} has already been used," +
                    " and indices must be used in ascending order! Please provide indices in" +
                    " ascending order or use auto-indices."
            )
        }
        // the "getNext" method must return the next FREE index,
        // since this one was already used we have to use the next one.
        state = value + 1
        return value
    }

    fun getExplicitOrNextFree(explicitIndex: Int?) = if (explicitIndex == null) {
        getNextFreeIndex()
    } else {
        useFixedIndex(explicitIndex)
    }

}