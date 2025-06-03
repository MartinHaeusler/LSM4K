package org.chronos.chronostore.api.statistics

/**
 * An immutable snapshot report containing statistics about a single cache.
 */
data class CacheStatisticsReport(
    /** How many requests have been made for the cache? */
    val requests: Long,

    /** How many cache misses have occurred in the block cache? */
    val misses: Long,

    /** How many cache evictions have occurred in the block cache? */
    val evictions: Long,
) {

    val hits: Long = this.requests - this.misses

    val hitRate: Double = if (this.requests == 0L) {
        1.0
    } else {
        this.hits.toDouble() / this.requests
    }

}