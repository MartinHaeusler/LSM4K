package io.github.martinhaeusler.lsm4k.io.vfs.disk

class DiskBasedVirtualFileSystemSettings(
    val fileSyncMode: FileSyncMode = FileSyncMode.CHANNEL_DATASYNC,
)