package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.api.statistics.CacheStatisticsReport
import java.util.concurrent.atomic.AtomicLong

/**
 * Collects statistics about a single cache.
 */
class CacheStatisticsCollector {

    /** How many requests have been made for the cache? */
    val requests = AtomicLong(0)

    /** How many cache misses have occurred in the block cache? */
    val misses = AtomicLong(0)

    /** How many cache evictions have occurred in the block cache? */
    val evictions = AtomicLong(0)

    fun reportCacheRequest() {
        this.requests.incrementAndGet()
    }

    fun reportCacheMiss() {
        this.misses.incrementAndGet()
    }

    fun reportCacheEviction() {
        this.evictions.incrementAndGet()
    }

    fun report(): CacheStatisticsReport {
        return CacheStatisticsReport(
            requests = this.requests.get(),
            misses = this.misses.get(),
            evictions = this.evictions.get(),
        )
    }

    fun reset() {
        this.requests.set(0)
        this.misses.set(0)
        this.evictions.set(0)
    }

}