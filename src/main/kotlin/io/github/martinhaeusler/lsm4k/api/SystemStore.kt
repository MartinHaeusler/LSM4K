package io.github.martinhaeusler.lsm4k.api

import io.github.martinhaeusler.lsm4k.util.StoreId

/**
 * [SystemStore]s are predefined [Store]s which are intended for system-internal purposes.
 */
enum class SystemStore {

    ;

    companion object {

        const val PATH_PREFIX = "_system_"

    }

    abstract val storeId: StoreId

}

private fun systemStorePath(vararg path: String): StoreId {
    require(path.isNotEmpty()) { "System store paths must not be empty!" }
    return StoreId.of(SystemStore.PATH_PREFIX, *path)
}