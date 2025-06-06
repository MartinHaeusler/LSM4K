package org.lsm4k.util

import kotlin.experimental.ExperimentalTypeInference

object GroupingExtensions {

    fun <T, K> Grouping<T, K>.toLists(): Map<K, List<T>> {
        return this.aggregate { _, accumulator: MutableList<T>?, element, _ ->
            val target = accumulator ?: mutableListOf()
            target.add(element)
            target
        }
    }

    fun <T, K> Grouping<T, K>.toSets(): Map<K, Set<T>> {
        return this.aggregate { _, accumulator: MutableSet<T>?, element, _ ->
            val target = accumulator ?: mutableSetOf()
            target.add(element)
            target
        }
    }

    inline fun <T, K, E> Grouping<T, K>.toLists(crossinline extract: (T) -> E): Map<K, List<E>> {
        return this.aggregate { _, accumulator: MutableList<E>?, element, _ ->
            val target = accumulator ?: mutableListOf()
            target.add(extract(element))
            target
        }
    }

    inline fun <T, K, E> Grouping<T, K>.toSets(crossinline extract: (T) -> E): Map<K, Set<E>> {
        return this.aggregate { _, accumulator: MutableSet<E>?, element, _ ->
            val target = accumulator ?: mutableSetOf()
            target.add(extract(element))
            target
        }
    }

    @OptIn(ExperimentalTypeInference::class)
    @OverloadResolutionByLambdaReturnType
    @JvmName("sumOfInt")
    inline fun <T, K> Grouping<T, K>.sumOf(extract: (T) -> Int): Map<K, Int> {
        return this.aggregate { _, accumulator: Int?, element, _ ->
            (accumulator ?: 0) + extract(element)
        }
    }

    @OptIn(ExperimentalTypeInference::class)
    @OverloadResolutionByLambdaReturnType
    @JvmName("sumOfLong")
    fun <T, K> Grouping<T, K>.sumOf(extract: (T) -> Long): Map<K, Long> {
        return this.aggregate { _, accumulator: Long?, element, _ ->
            (accumulator ?: 0L) + extract(element)
        }
    }

    @OptIn(ExperimentalTypeInference::class)
    @OverloadResolutionByLambdaReturnType
    @JvmName("sumOfDouble")
    fun <T, K> Grouping<T, K>.sumOf(extract: (T) -> Double): Map<K, Double> {
        return this.aggregate { _, accumulator: Double?, element, _ ->
            (accumulator ?: 0.0) + extract(element)
        }
    }

}