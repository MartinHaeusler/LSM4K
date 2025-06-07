package io.github.martinhaeusler.lsm4k.impl.transaction

import io.github.martinhaeusler.lsm4k.api.GetResult
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes

class GetResultImpl(
    override val key: Bytes,
    override val isHit: Boolean,
    override val isModifiedInTransactionContext: Boolean,
    override val lastModificationTSN: TSN?,
    override val value: Bytes?,
) : GetResult