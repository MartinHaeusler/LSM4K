package org.lsm4k.manifest

import org.lsm4k.impl.annotations.PersistentClass
import org.lsm4k.util.FileIndex
import org.lsm4k.util.LevelOrTierIndex

@PersistentClass(format = PersistentClass.Format.JSON, details = "Stored in manifest.")
data class LSMFileInfo(
    /** The index of the file.*/
    val fileIndex: FileIndex,
    /** The number of the LSM Level (in Leveled Compaction) or LSM Tier (in Tiered Compaction). */
    val levelOrTier: LevelOrTierIndex,
)