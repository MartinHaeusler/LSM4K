package org.chronos.chronostore.api

import org.chronos.chronostore.util.InverseQualifiedTemporalKey
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.bytes.Bytes

/**
 * [SystemStore]s are predefined [Store]s which are intended for system-internal purposes.
 */
enum class SystemStore {

    /**
     * The commit metadata table stores the metadata (i.e. "commit message") for each commit.
     *
     * The table layout is defined as follows:
     *
     * - **Key**: The commit timestamp (64-bit Long)
     * - **Value**: The commit metadata, as [Bytes]
     */
    COMMIT_METADATA {

        override val isVersioned = false

        override val storeId = systemStorePath("commit-metadata")

    },

    /**
     * The commit log table stores information about which data was modified by a commit.
     *
     * The table layout is defined as follows:
     *
     * - **Key**: An [InverseQualifiedTemporalKey], as bytes.
     * - **Value**: 0 if the action was a [Command.OpCode.DEL], 1 if the action was a [Command.OpCode.PUT].
     */
    COMMIT_LOG {

        override val isVersioned = false

        override val storeId = systemStorePath("commit-log")

    },

    ;

    companion object {

        const val PATH_PREFIX = "_system_"

    }

    abstract val isVersioned: Boolean

    abstract val storeId: StoreId

}

private fun systemStorePath(vararg path: String): StoreId {
    require(path.isNotEmpty()) { "System store paths must not be empty!" }
    return StoreId.of(SystemStore.PATH_PREFIX, *path)
}