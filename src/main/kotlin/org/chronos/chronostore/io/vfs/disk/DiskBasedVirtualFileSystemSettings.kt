package org.chronos.chronostore.io.vfs.disk

class DiskBasedVirtualFileSystemSettings(
    val fileSyncMode: FileSyncMode = FileSyncMode.CHANNEL_DATASYNC,
)