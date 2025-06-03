package org.chronos.chronostore.api.statistics

/**
 * Collects runtime statistics and reports upon them.
 */
interface StatisticsManager {

    /** Indicates if statistics are currently being collected. */
    val isCollectionActive: Boolean

    /**
     * Starts the collecting statistics.
     *
     * This method will have no effect if there already is an ongoing collection of statistics.
     *
     * @return `true` if the collection of statistics was started, `false` if it was already ongoing.
     */
    fun startCollection(): Boolean

    /**
     * Resets the current statistics to zero (if there is an ongoing collection) and starts to collect statistics.
     */
    fun restartCollection()

    /**
     * Stops the collection of statistics.
     *
     * @return `true` if the collection of statistics was stopped, `false` if there was no ongoing collection.
     */
    fun stopCollection(): Boolean

    /**
     * Gets the most recent statistics report.
     *
     * @return The most recent statistics as an immutable report, or `null` if no statistics have ever been collected.
     */
    fun report(): StatisticsReport?

}