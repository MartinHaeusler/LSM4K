package io.github.martinhaeusler.lsm4k.lsm.cache

import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize
import io.github.oshai.kotlinlogging.KotlinLogging

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