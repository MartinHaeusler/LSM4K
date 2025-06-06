package org.lsm4k.impl.transaction

import org.lsm4k.api.GetResult
import org.lsm4k.util.TSN
import org.lsm4k.util.bytes.Bytes

class GetResultImpl(
    override val key: Bytes,
    override val isHit: Boolean,
    override val isModifiedInTransactionContext: Boolean,
    override val lastModificationTSN: TSN?,
    override val value: Bytes?
): GetResult