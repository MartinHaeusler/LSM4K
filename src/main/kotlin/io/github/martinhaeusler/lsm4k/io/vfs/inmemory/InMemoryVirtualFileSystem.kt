package io.github.martinhaeusler.lsm4k.io.vfs.inmemory

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystem
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystemElement
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes.Companion.writeBytesWithoutSize
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class InMemoryVirtualFileSystem : VirtualFileSystem {

    companion object {

        val PATH_PREFIX = "<memory>::"

    }

    /** Map from PATH to children FILENAMEs */
    private val fileTree: MutableMap<String, MutableSet<String>> = mutableMapOf()

    /** Map from PATH to file content */
    private val fileContents: MutableMap<String, Bytes> = mutableMapOf()

    private val fileSystemLock = ReentrantLock(true)

    override val rootPath: String
        get() = PATH_PREFIX

    override fun directory(name: String): VirtualDirectory {
        val pathElements = name.split(File.separator)

        val rootDir = InMemoryVirtualDirectory(this, pathElements.first())
        if (pathElements.size == 1) {
            return rootDir
        }

        var parentDir = rootDir
        for (index in 1..pathElements.lastIndex) {
            parentDir = InMemoryVirtualDirectory(parentDir, pathElements[index])
        }
        return parentDir
    }

    override fun file(name: String): VirtualReadWriteFile {
        val pathElements = name.split(File.separator)

        if (pathElements.size == 1) {
            return InMemoryVirtualReadWriteFile(null, this, pathElements.first())
        }

        val rootDir = InMemoryVirtualDirectory(this, pathElements.first())
        var parentDir = rootDir
        for (index in 1 until pathElements.lastIndex) {
            parentDir = InMemoryVirtualDirectory(parentDir, pathElements[index])
        }
        return InMemoryVirtualReadWriteFile(parentDir, this, pathElements.last())
    }

    override fun listRootLevelElements(): List<VirtualFileSystemElement> {
        val rootFiles = (this.fileTree[PATH_PREFIX] ?: emptySet()).map { this.file(it) }
        val rootDirs = this.fileTree.keys.asSequence()
            .filter { it != PATH_PREFIX }
            .map { it.removePrefix(PATH_PREFIX) }
            .filter { !it.contains(File.separator) }
            .map { this.directory(it) }
            .toList()
        return rootFiles + rootDirs
    }

    fun getFileLength(path: String): Long {
        return this.getFileContentOrNull(path)?.size?.toLong() ?: 0
    }

    fun getFileContentOrNull(path: String): Bytes? {
        return this.fileContents[path]
    }

    fun getFileContent(path: String): Bytes {
        return this.getFileContentOrNull(path)
            ?: throw IOException("Virtual File '${path}' does not exist!")
    }

    fun isExistingPath(path: String): Boolean {
        return this.fileTree.containsKey(path) || this.fileContents.containsKey(path)
    }

    fun isFile(path: String): Boolean {
        return this.fileContents.containsKey(path)
    }

    fun isDirectory(path: String): Boolean {
        return this.fileTree.containsKey(path)
    }

    fun listChildrenOfPath(path: String): List<String> {
        val children = fileTree[path]
            ?: emptyList()
        return children.toList()
    }

    fun mkdirs(path: String) {
        val pathElements = path.removePrefix(PATH_PREFIX).split(File.separatorChar)
        var pathSoFar = PATH_PREFIX
        var separator = ""
        for (pathElement in pathElements) {
            pathSoFar += (separator)
            separator = File.separator
            pathSoFar += (pathElement)
            this.mkdir(pathSoFar)
        }
    }

    /**
     * Equivalent to the unix command `mkdir`.
     *
     * @param path The path to create. The parent must exist.
     *
     * @throws IOException if the parent doesn't exist, or if there already is an element at the indicated path.
     */
    fun mkdir(path: String) {
        val pathWithoutPrefix = path.removePrefix(PATH_PREFIX)
        if (pathWithoutPrefix.contains(File.separator)) {
            val parent = getParentPath(path)
            val child = pathWithoutPrefix.substringAfterLast(File.separatorChar)
            // "mkdir" is an atomic operation on most file systems, so let's make it atomic here as well.
            this.fileSystemLock.withLock {
                if (this.isFile(path)) {
                    throw IOException("Cannot create directory '${path}' - it refers to a file!")
                }
                if (parent != PATH_PREFIX) {
                    if (!isDirectory(parent)) {
                        throw IOException("Cannot create directory '${path}' - its parent doesn't exist!")
                    }
                }
                fileTree.getOrPut(parent, ::mutableSetOf) += child
                fileTree.getOrPut(path, ::mutableSetOf)
            }
        } else {
            // create the root directory
            this.fileSystemLock.withLock {
                fileTree.getOrPut(path, ::mutableSetOf)
            }
        }
    }

    /**
     * Equivalent to the unix command `touch`.
     *
     * @param path The path to create the new file at.
     *
     * @throws IOException if the [path] already refers to an existing file or directory.
     */
    fun createNewFile(path: String) {
        // creating a new file is an atomic operation on most file systems, so let's make it atomic here as well.
        this.fileSystemLock.withLock {
            val parent = path.substringBeforeLast(File.separator, PATH_PREFIX)
            if (parent != PATH_PREFIX) {
                if (!this.isDirectory(parent)) {
                    throw IOException("Cannot create file '${path}' - the parent directory does not exist!")
                }
            }
            if (this.isExistingPath(path)) {
                throw IOException("Cannot create file '${path}' - there already is an existing element at this path!")
            }
            val fileName = path.removePrefix(parent).removePrefix(File.separator)
            this.fileContents[path] = Bytes.EMPTY
            this.fileTree.getOrPut(parent, ::mutableSetOf) += fileName
        }
    }

    /**
     * Equivalent to unix command `rm`.
     *
     * @param path The path to delete. If it refers to a directory, it must be empty.
     *
     * @throws IOException if the [path] refers to a non-empty directory.
     */
    fun delete(path: String) {
        this.fileSystemLock.withLock {
            if (this.isDirectory(path)) {
                if (this.listChildrenOfPath(path).isNotEmpty()) {
                    throw IOException("Cannot delete directory '${path}' - it is not empty!")
                }
                this.fileTree.remove(path)
                val parent = getParentPath(path)
                this.fileTree[parent]?.remove(getLeafName(path))
                return
            }
            if (this.isFile(path)) {
                this.fileContents.remove(path)
                this.fileTree[path.substringBeforeLast(File.separatorChar)]?.remove(path.substringAfterLast(File.separatorChar))
                return
            }
        }
    }

    fun deleteIfExists(path: String): Boolean {
        this.fileSystemLock.withLock {
            if (!this.isExistingPath(path)) {
                return false
            }
            this.delete(path)
            return true
        }
    }

    private fun getParentPath(path: String): String {
        return PATH_PREFIX + path.removePrefix(PATH_PREFIX).substringBeforeLast(File.separatorChar, missingDelimiterValue = "")
    }

    private fun getLeafName(path: String): String {
        return path.removePrefix(PATH_PREFIX).substringAfterLast(File.separatorChar, missingDelimiterValue = "")
    }

    fun truncateFile(path: String, bytesToKeep: Long) {
        require(bytesToKeep >= 0) { "Argument 'bytesToKeep' (${bytesToKeep}) must not be negative!" }
        require(bytesToKeep <= Int.MAX_VALUE) { "Argument 'bytesToKeep' (${bytesToKeep}) is too large for the in-memory Virtual File System!" }
        this.fileSystemLock.withLock {
            check(this.isFile(path)) { "The given path is not an existing file: '${path}'!" }
            this.fileContents[path] = Bytes.wrap(this.fileContents.getValue(path).slice(0, bytesToKeep.toInt()).toOwnedArray())
        }
    }


    fun overwrite(path: String, bytes: Bytes) {
        // overwrites can be made atomic by creating a temporary secondary file,
        // writing the desired content to the secondary file, and renaming the
        // secondary file to the primary file. So let's consider it as an atomic operation here.
        this.fileSystemLock.withLock {
            if (!this.isFile(path)) {
                throw IOException("Cannot overwrite file '${path}' - the path does not refer to an existing file!")
            }
            // ensure that each file has its own array
            this.fileContents[path] = BasicBytes(bytes.toOwnedArray())
        }
    }

    fun openAppendOutputStream(path: String): OutputStream {
        if (!this.isFile(path)) {
            throw IOException("Path '${path}' does not refer to an existing file!")
        }
        val outputStream = ByteArrayOutputStream()
        val existingContent = this.getFileContentOrNull(path) ?: Bytes.EMPTY
        outputStream.writeBytesWithoutSize(existingContent)
        return ObservableOutputStream(outputStream) {
            this.overwrite(path, Bytes.wrap(outputStream.toByteArray()))
        }
    }

    private class ObservableOutputStream(
        private val outputStream: OutputStream,
        private val onChange: () -> Unit,
    ) : OutputStream() {

        override fun flush() {
            super.flush()
            this.outputStream.flush()
            this.onChange()
        }

        override fun close() {
            super.close()
            this.outputStream.close()
            this.onChange()
        }

        override fun write(b: ByteArray) {
            this.outputStream.write(b)
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            this.outputStream.write(b, off, len)
        }

        override fun write(b: Int) {
            this.outputStream.write(b)
        }

    }

    override fun toString(): String {
        return PATH_PREFIX
    }

}