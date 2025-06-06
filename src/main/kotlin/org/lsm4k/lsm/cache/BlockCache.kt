package org.lsm4k.lsm.cache

import io.github.oshai.kotlinlogging.KotlinLogging
import org.lsm4k.io.format.BlockLoader
import org.lsm4k.util.statistics.StatisticsReporter
import org.lsm4k.util.unit.BinarySize

sealed interface BlockCache : BlockLoader {

    companion object {

        private val log = KotlinLogging.logger {}

        fun create(maxSize: BinarySize?, loader: BlockLoader, statisticsReporter: StatisticsReporter): BlockCache {
            return if (maxSize == null || maxSize.bytes <= 0) {
                log.warn { "Block Cache has been disabled in Database Engine configuration. This may lead to poor read performance - do not use in production!" }
                NoBlockCache(loader)
            } else {
                log.info { "Block Cache: ${maxSize.toHumanReadableString()}" }
                BlockCacheImpl(
                    maxSize = maxSize,
                    loader = loader,
                    statisticsReporter = statisticsReporter,
                )
            }
        }

        fun none(loader: BlockLoader): BlockCache {
            return NoBlockCache(loader)
        }

    }


}