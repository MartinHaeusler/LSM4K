package io.github.martinhaeusler.lsm4k.io.vfs.disk

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystemElement
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.util.OperatingSystem
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption

class DiskBasedVirtualDirectory(
    override val parent: DiskBasedVirtualDirectory?,
    private val file: File,
    private val vfs: DiskBasedVirtualFileSystem,
) : VirtualDirectory {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    override fun list(): List<String> {
        return this.file.list()?.asList() ?: emptyList()
    }

    override fun listElements(): List<VirtualFileSystemElement> {
        return this.file.listFiles()?.map {
            if (it.isFile) {
                DiskBasedVirtualReadWriteFile(this, it, this.vfs)
            } else {
                DiskBasedVirtualDirectory(this, it, this.vfs)
            }
        } ?: emptyList()
    }

    override fun mkdirs() {
        if (!this.file.exists()) {
            this.file.mkdirs()
        }
    }

    override fun clear() {
        for (file in this.file.listFiles() ?: emptyArray()) {
            file.deleteRecursively()
        }
    }

    override fun delete() {
        Files.delete(this.file.toPath())
    }

    override fun file(name: String): VirtualReadWriteFile {
        return DiskBasedVirtualReadWriteFile(this, File(file, name), this.vfs)
    }

    override fun directory(name: String): VirtualDirectory {
        return DiskBasedVirtualDirectory(this, File(this.file, name), this.vfs)
    }

    override val name: String
        get() = this.file.name

    override val path: String
        get() = this.file.absolutePath

    override fun exists(): Boolean {
        return this.file.exists() && this.file.isDirectory
    }

    override fun toString(): String {
        return "Dir[${this.path}]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DiskBasedVirtualDirectory

        return file == other.file
    }

    override fun hashCode(): Int {
        return file.hashCode()
    }

    override fun fsync() {
        if (this.vfs.settings.fileSyncMode == FileSyncMode.NO_SYNC) {
            return
        }

        if (OperatingSystem.isWindows) {
            // fsync on directories is neither needed nor supported on windows.
            return
        }

        // TECHNICALLY it's not officially supported to fsync a directory
        // in Java (and it also doesn't work on windows). However, many
        // Java databases rely on this functionality already, so we're not
        // the first to do this kind of hack...
        try {
            FileChannel.open(this.file.toPath(), StandardOpenOption.READ).use { channel ->
                // even though we're in READ mode, we can still force the channel, which
                // will lead to a call to fsync().
                channel.force(true)
            }
        } catch (e: Exception) {
            log.warn(e) {
                "Could not perform fsync() on directory '${this.path}'." +
                    " This may cause data corruption in case of power outage or immediate OS shutdowns." +
                    " Cause: ${e}"
            }
        }

    }

}