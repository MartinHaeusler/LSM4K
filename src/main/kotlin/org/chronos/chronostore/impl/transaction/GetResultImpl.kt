package org.chronos.chronostore.impl.transaction

import org.chronos.chronostore.api.GetResult
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.bytes.Bytes

class GetResultImpl(
    override val key: Bytes,
    override val isHit: Boolean,
    override val isModifiedInTransactionContext: Boolean,
    override val lastModificationTSN: TSN?,
    override val value: Bytes?
): GetResult