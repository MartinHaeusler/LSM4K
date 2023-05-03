package org.chronos.chronostore.api

import org.chronos.chronostore.util.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit


class TimeOfDay private constructor(
    val hour: Int,
    val minute: Int,
    val second: Int,
    val millisecond: Int
) {

    companion object {

        fun twentyFourHourFormat(hour: Int, minute: Int, second: Int, millisecond: Int): TimeOfDay {
            require(hour in 0..23) { "Argument 'hour' (${hour}) must be in range [0..23]!" }
            require(minute in 0..59) { "Argument 'minute' (${minute}) must be in range [0..59]!" }
            require(second in 0..59) { "Argument 'second' (${second}) must be in range [0..59]!" }
            require(millisecond in 0..999) { "Argument 'millisecond' (${millisecond}) must be in range [0.999]!" }
            return TimeOfDay(hour, minute, second, millisecond)
        }

        fun am(hour: Int, minute: Int, second: Int, millisecond: Int): TimeOfDay {
            require(hour in 0..11) { "Argument 'hour' (${hour}) must be in range [0..11]!" }
            require(minute in 0..59) { "Argument 'minute' (${minute}) must be in range [0..59]!" }
            require(second in 0..59) { "Argument 'second' (${second}) must be in range [0..59]!" }
            require(millisecond in 0..999) { "Argument 'millisecond' (${millisecond}) must be in range [0.999]!" }
            return twentyFourHourFormat(hour, minute, second, millisecond)
        }

        fun pm(hour: Int, minute: Int, second: Int, millisecond: Int): TimeOfDay {
            require(hour in 0..11) { "Argument 'hour' (${hour}) must be in range [0..11]!" }
            require(minute in 0..59) { "Argument 'minute' (${minute}) must be in range [0..59]!" }
            require(second in 0..59) { "Argument 'second' (${second}) must be in range [0..59]!" }
            require(millisecond in 0..999) { "Argument 'millisecond' (${millisecond}) must be in range [0.999]!" }
            return twentyFourHourFormat(hour + 12, minute, second, millisecond)
        }

        private val regex = """([0-9][0-9])(:([0-9][0-9])(:([0-9][0-9])(.([0-9]{1,3}))?)?)?\s*(am|pm)?""".toRegex()

        fun parseOrNull(value: String): TimeOfDay? {
            val match = regex.matchEntire(value.trim().lowercase())
                ?: return null
            val hourString = match.groupValues[1] // hour
            val minuteString = match.groupValues[3].takeIf { it.isNotBlank() } // minute|null
            val secondString = match.groupValues[5].takeIf { it.isNotBlank() } // second|null
            val millisString = match.groupValues[7].takeIf { it.isNotBlank() } // millis|null
            val amPmString = match.groupValues[8].takeIf { it == "am" || it == "pm" } // am|pm|null

            val hour = hourString.toIntOrNull()?.takeIf { it in 0..23 }
                ?: return null

            val minute = if (minuteString != null) {
                minuteString.toIntOrNull()?.takeIf { it in 0..59 }
                    ?: return null
            } else {
                0
            }
            val second = if (secondString != null) {
                secondString.toIntOrNull()?.takeIf { it in 0..59 }
                    ?: return null
            } else {
                0
            }
            val millisecond = if (millisString != null) {
                millisString.toIntOrNull()?.takeIf { it in 0..999 }
                    ?: return null
            } else {
                0
            }

            return when (amPmString) {
                null -> twentyFourHourFormat(hour, minute, second, millisecond)
                "am" -> if (hour in 0..11) {
                    am(hour, minute, second, millisecond)
                } else {
                    null
                }

                "pm" -> if (hour in 0..11) {
                    pm(hour, minute, second, millisecond)
                } else {
                    null
                }

                else -> null
            }
        }

        fun parse(value: String): TimeOfDay? {
            val match = regex.matchEntire(value.trim().lowercase())
                ?: throw IllegalArgumentException("Could not parse time-of-day from '${value}'. Please use the following format: hh:mm:ss.SSS (am|pm)?")
            val hourString = match.groupValues[1].takeIf { it.isNotBlank() } // hour
            val minuteString = match.groupValues[3].takeIf { it.isNotBlank() } // minute|null
            val secondString = match.groupValues[5].takeIf { it.isNotBlank() } // second|null
            val millisString = match.groupValues[7].takeIf { it.isNotBlank() } // millis|null
            val amPmString = match.groupValues[8].takeIf { it == "am" || it == "pm" } // am|pm|null

            val hour = hourString?.toIntOrNull()?.takeIf { it in 0..59 }
                ?: throw IllegalArgumentException("Could not parse hour from: '${value}'")

            val minute = if (minuteString != null) {
                minuteString.toIntOrNull()?.takeIf { it in 0..59 }
                    ?: throw IllegalArgumentException("Could not parse minute from: '${value}'")
            } else {
                0
            }
            val second = if (secondString != null) {
                secondString.toIntOrNull()?.takeIf { it in 0..59 }
                    ?: throw IllegalArgumentException("Could not parse second from: '${value}'")
            } else {
                0
            }
            val millisecond = if (millisString != null) {
                millisString.toIntOrNull()?.takeIf { it in 0..999 }
                    ?: throw IllegalArgumentException("Could not parse millisecond from: '${value}'")
            } else {
                0
            }

            return when (amPmString) {
                null -> twentyFourHourFormat(hour, minute, second, millisecond)
                "am" -> if (hour in 0..11) {
                    am(hour, minute, second, millisecond)
                } else {
                    throw IllegalArgumentException("Cannot use 'am' format: hour (${hour}) is not within range [0..11]!")
                }

                "pm" -> if (hour in 0..11) {
                    pm(hour, minute, second, millisecond)
                } else {
                    throw IllegalArgumentException("Cannot use 'pm' format: hour (${hour}) is not within range [0..11]!")
                }

                else -> throw IllegalArgumentException("Could not detect time-of-day format! Please use either 'am', 'pm' or nothing (for 24-hour format)!")
            }
        }

    }

    val timestampToday: Timestamp
        get() {
            val systemZone = OffsetDateTime.now().offset
            val startOfDay = LocalDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT).toInstant(systemZone)
            return startOfDay.toEpochMilli() +
                TimeUnit.HOURS.toMillis(this.hour.toLong()) +
                TimeUnit.MINUTES.toMillis(this.minute.toLong()) +
                TimeUnit.SECONDS.toMillis(this.second.toLong()) +
                this.millisecond
        }

}