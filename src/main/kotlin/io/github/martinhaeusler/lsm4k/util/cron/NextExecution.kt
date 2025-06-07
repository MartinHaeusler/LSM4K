package io.github.martinhaeusler.lsm4k.util.cron

import java.time.ZonedDateTime

/**
 * The next execution date of a Cron expression.
 */
data class NextExecution(
    /** The number of milliseconds until the next execution occurs (at the time of computation). */
    val delayInMillis: Long,
    /** The next execution date as an absolute date. */
    val nextDateTime: ZonedDateTime,
)