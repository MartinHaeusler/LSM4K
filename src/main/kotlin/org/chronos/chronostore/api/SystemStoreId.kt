package org.chronos.chronostore.api

import org.chronos.chronostore.util.InverseQualifiedTemporalKey
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.StoreId
import java.util.*

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

        override val isVersioned: Boolean = false

        override val id: StoreId = UUID.fromString("11111111-1111-1111-1111-000000000001")

        override val storeName: String = "${SystemStore.NAME_PREFIX}CommitMetadata"

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

        override val isVersioned: Boolean = false

        override val id: StoreId = UUID.fromString("11111111-1111-1111-1111-000000000002")

        override val storeName: String = "${SystemStore.NAME_PREFIX}CommitLog"

    },

    ;

    companion object {

        const val NAME_PREFIX = "_chronostore_internal__"

    }

    abstract val isVersioned: Boolean

    abstract val id: StoreId

    abstract val storeName: String

}