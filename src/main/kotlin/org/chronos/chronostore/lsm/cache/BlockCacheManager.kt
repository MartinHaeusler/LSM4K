package org.chronos.chronostore.lsm.cache

import mu.KotlinLogging
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.unit.BinarySize

sealed interface BlockCacheManager {

    fun getBlockCache(storeId: StoreId): LocalBlockCache

    companion object {

        private val log = KotlinLogging.logger {}

        fun create(maxSize: BinarySize?): BlockCacheManager {
            return if (maxSize == null || maxSize.bytes <= 0) {
                log.warn { "Block Cache has been disabled in ChronoStore configuration. This may lead to poor read performance - do not use in production!" }
                NoBlockCacheManager
            } else {
                log.info { "Block Cache: ${maxSize.toHumanReadableString()}" }
                BlockCacheManagerImpl(maxSize)
            }
        }

    }


}