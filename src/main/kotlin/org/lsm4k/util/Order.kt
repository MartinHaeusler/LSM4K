package org.lsm4k.util

enum class Order {

    ASCENDING {

        override val inverse: Order
            get() = DESCENDING

    },

    DESCENDING {

        override val inverse: Order
            get() = ASCENDING

    };

    abstract val inverse: Order

    fun applyToCompare(compare: Int): Int {
        return when (this) {
            ASCENDING -> compare
            DESCENDING -> compare * -1
        }

    }

    fun toInt(): Int {
        return when(this){
            ASCENDING -> 1
            DESCENDING -> -1
        }
    }


}