package org.chronos.chronostore.manifest

import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.LevelOrTierIndex

@PersistentClass(format = PersistentClass.Format.JSON, details = "Stored in manifest.")
data class LSMFileInfo(
    /** The index of the file.*/
    val fileIndex: FileIndex,
    /** The number of the LSM Level (in Leveled Compaction) or LSM Tier (in Tiered Compaction). */
    val levelOrTier: LevelOrTierIndex,
)