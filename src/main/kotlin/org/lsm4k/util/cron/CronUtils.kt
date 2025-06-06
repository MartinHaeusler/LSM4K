package org.lsm4k.util.cron

import com.cronutils.model.Cron
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import kotlin.jvm.optionals.getOrNull

object CronUtils {

    private val CRON_FORMAT = CronType.SPRING53
    private val CRON_DEFINITION = CronDefinitionBuilder.instanceDefinitionFor(CRON_FORMAT)
    private val CRON_PARSER = CronParser(CRON_DEFINITION)

    fun cron(definition: String): Cron {
        return CRON_PARSER.parse(definition)
    }

    fun Cron.isValid(): Boolean {
        return try {
            this.validate()
            true
        } catch (e: IllegalArgumentException) {
            false
        }
    }

    fun Cron.nextExecution(lastExecution: ZonedDateTime = ZonedDateTime.now()): NextExecution? {
        val durationToNextExecution = ExecutionTime.forCron(this).timeToNextExecution(lastExecution).getOrNull()
        return if (durationToNextExecution == null || durationToNextExecution.isNegative) {
            null
        } else {
            val delay = durationToNextExecution.toMillis()
            val next = lastExecution.plus(delay, ChronoUnit.MILLIS)
            return NextExecution(delay, next)
        }
    }

}