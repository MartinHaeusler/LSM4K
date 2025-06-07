package io.github.martinhaeusler.lsm4k.manifest

import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.LevelOrTierIndex

@PersistentClass(format = PersistentClass.Format.JSON, details = "Stored in manifest.")
data class LSMFileInfo(
    /** The index of the file.*/
    val fileIndex: FileIndex,
    /** The number of the LSM Level (in Leveled Compaction) or LSM Tier (in Tiered Compaction). */
    val levelOrTier: LevelOrTierIndex,
)