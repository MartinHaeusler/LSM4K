package io.github.martinhaeusler.lsm4k.api.compaction

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass.Format
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB

@PersistentClass(format = Format.JSON, details = "Used in Manifest")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(
        value = FileSeparationStrategy.SingleFile::class,
        name = "singleFile",
    ),
    JsonSubTypes.Type(
        value = FileSeparationStrategy.SizeBased::class,
        name = "sizeBased",
    ),
)
sealed interface FileSeparationStrategy {

    /**
     * Does not split the files at all, creating a single file per tier/level (except for tier/level 0).
     *
     * **IMPORTANT:** This is generally **not** recommended due to the negative impact on compaction performance.
     * Use this only if you need to minimize the number of files per store.
     */
    @PersistentClass(format = Format.JSON, details = "Used in Manifest")
    class SingleFile : FileSeparationStrategy {

        override fun equals(other: Any?): Boolean {
            return this === other
        }

        override fun hashCode(): Int {
            return System.identityHashCode(this)
        }

    }


    /**
     * The target size individual files should have after compaction.
     *
     * This is a soft limit. If individual key-value pairs are very large, this limit may be exceeded.
     * Also, if there's not enough data to fill a file, individual files may be considerably smaller
     * than this.
     *
     * Generally speaking, the stream of key-value pairs to be written gets split into a new file
     * whenever this limit is exceeded.
     *
     * Compared to [SingleFile], this setting can speed up lookups and can work around maximum file
     * size limitations imposed by the file system.
     *
     * This is the default choice.
     */
    @PersistentClass(format = Format.JSON, details = "Used in Manifest")
    data class SizeBased(
        val individualFileSize: BinarySize = 512.MiB,
    ) : FileSeparationStrategy

    // TODO [FEATURE]: Support spooky separation algorithm (https://nivdayan.github.io/spooky.pdf)

}


