package io.github.martinhaeusler.lsm4k.test.util.fakestoredsl

import io.github.martinhaeusler.lsm4k.api.compaction.CompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.TransactionId

@FakeStoreDSL
interface FakeStoreConfigurator {

    /**
     * From which [TSN] onward is this store valid?
     *
     * Defaults to `0`, which means "valid from the start".
     */
    var validFromTSN: TSN

    /**
     * Up until which [TSN] is this store valid?
     *
     * Defaults to `null`, which means "valid forever".
     */
    var validToTSN: TSN?

    /**
     * Which transaction created this store?
     *
     * Defaults to a random ID.
     */
    var createdByTransactionId: TransactionId

    /**
     * Which compaction strategy should be used in the store?
     *
     * Defaults to a standard [LeveledCompactionStrategy].
     */
    var compactionStrategy: CompactionStrategy

    fun level(level: Int? = null, configure: LevelOrTierConfigurator.() -> Unit = {})

    fun tier(tier: Int? = null, configure: LevelOrTierConfigurator.() -> Unit = {})

}