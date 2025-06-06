package org.lsm4k.api.compaction

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.lsm4k.impl.annotations.PersistentClass

@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest.")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(
        value = LeveledCompactionStrategy::class,
        name = "leveled",
    ),
    JsonSubTypes.Type(
        value = TieredCompactionStrategy::class,
        name = "tiered",
    ),
)
sealed interface CompactionStrategy {

    companion object {

        /**
         * Defaults to a leveled compaction strategy.
         */
        val DEFAULT = LeveledCompactionStrategy()

    }

    val fileSeparationStrategy: FileSeparationStrategy

}

