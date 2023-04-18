package org.chronos.chronostore.api

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.util.StoreId
import java.util.*

interface Store {

    val id: StoreId

    var name: String

    val retainOldVersions: Boolean

    val directory: VirtualDirectory

}