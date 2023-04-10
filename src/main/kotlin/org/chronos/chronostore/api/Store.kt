package org.chronos.chronostore.api

import org.chronos.chronostore.io.vfs.VirtualDirectory
import java.util.*

interface Store {

    val id: UUID

    val name: String

    val retainOldVersions: Boolean

    val directory: VirtualDirectory

}