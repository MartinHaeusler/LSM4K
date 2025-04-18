package org.chronos.chronostore.lsm.cache

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.io.format.BlockLoader
import org.chronos.chronostore.util.unit.BinarySize

sealed interface BlockCache : BlockLoader {

    companion object {

        private val log = KotlinLogging.logger {}

        fun create(maxSize: BinarySize?, loader: BlockLoader): BlockCache {
            return if (maxSize == null || maxSize.bytes <= 0) {
                log.warn { "Block Cache has been disabled in ChronoStore configuration. This may lead to poor read performance - do not use in production!" }
                NoBlockCache(loader)
            } else {
                log.info { "Block Cache: ${maxSize.toHumanReadableString()}" }
                BlockCacheImpl(maxSize, loader)
            }
        }

        fun none(loader: BlockLoader): BlockCache {
            return NoBlockCache(loader)
        }

    }


}