package org.lsm4k.util

object ListExtensions {

    fun <T> List<T>?.headTailOrNull(): Pair<T, List<T>>? {
        return when(this?.size){
            null, 0 -> null
            1 -> Pair(this.first(), emptyList())
            else -> Pair(this.first(), this.drop(1))
        }
    }

    fun <T> List<T>.headTail(): Pair<T, List<T>> {
        return this.headTailOrNull()
            ?: throw IllegalArgumentException("List is empty, cannot separate into head/tail!")
    }

}