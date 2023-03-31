package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Timestamp
import java.util.UUID

class FileMetaData(
    val settings: ChronoStoreFileSettings,
    val fileUUID: UUID,
    val minKey: Bytes,
    val maxKey: Bytes,
    val minTimestamp: Timestamp,
    val maxTimestamp: Timestamp,
    val createdAt: Timestamp,
    val infoMap: Map<Bytes, Bytes>
) {

}