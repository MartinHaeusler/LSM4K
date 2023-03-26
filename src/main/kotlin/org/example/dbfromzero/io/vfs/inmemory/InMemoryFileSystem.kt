package org.example.dbfromzero.io.vfs.inmemory

import org.example.dbfromzero.io.vfs.VirtualDirectory
import org.example.dbfromzero.io.vfs.VirtualFile
import org.example.dbfromzero.util.Bytes
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class InMemoryFileSystem {

    /** Map from PATH to children FILENAMEs */
    private val fileTree: MutableMap<String, MutableSet<String>> = mutableMapOf()

    /** Map from PATH to file content */
    private val fileContents: MutableMap<String, Bytes> = mutableMapOf()

    private val fileSystemLock = ReentrantLock(true)

    fun directory(path: String): VirtualDirectory {
        val pathElements = path.split(File.separator)

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

    fun file(path: String): VirtualFile {
        val pathElements = path.split(File.separator)

        val rootDir = InMemoryVirtualDirectory(this, pathElements.first())
        if (pathElements.size == 1) {
            throw IOException("Virtual files at root level are not allowed.")
        }

        var parentDir = rootDir
        for (index in 1 until pathElements.lastIndex) {
            parentDir = InMemoryVirtualDirectory(parentDir, pathElements[index])
        }
        return InMemoryVirtualFile(parentDir, pathElements.last())
    }

    fun getFileLength(path: String): Long {
        return this.getFileContentOrNull(path)?.size?.toLong() ?: -1
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
        val pathElements = path.split(File.separatorChar)
        var pathSoFar = ""
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
        val parent = path.substringBeforeLast(File.separator)
        val child = path.substring(parent.length + 1)
        // "mkdir" is an atomic operation on most file systems, so let's make it atomic here as well.
        this.fileSystemLock.withLock {
            if (this.isFile(path)) {
                throw IOException("Cannot create directory '${path}' - it refers to a file!")
            }
            if (parent.isNotEmpty()) {
                if (!isDirectory(parent)) {
                    throw IOException("Cannot create directory '${path}' - its parent doesn't exist!")
                }
            }
            fileTree.getOrPut(parent, ::mutableSetOf) += child
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
            val parent = path.substringBeforeLast(File.separator)
            if (!this.isDirectory(parent)) {
                throw IOException("Cannot create file '${path}' - the parent directory does not exist!")
            }
            if (this.isExistingPath(path)) {
                throw IOException("Cannot create file '${path}' - there already is an existing element at this path!")
            }
            this.fileContents[path] = Bytes.EMPTY
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
            }
            if (this.isFile(path)) {
                this.fileContents.remove(path)
            }
        }
    }

    fun overwrite(path: String, bytes: Bytes) {
        // overwrites can be made atomic by creating a temporary secondary file,
        // writing the desired content to the secondary file, and renaming the
        // secondary file to the primary file. So let's consider it as an atomic operation here.
        this.fileSystemLock.withLock {
            if(!this.isFile(path)){
                throw IOException("Cannot overwrite file '${path}' - the path does not refer to an existing file!")
            }
            this.fileContents[path] = bytes
        }
    }

    fun openAppendOutputStream(path: String): OutputStream {
        if(!this.isFile(path)){
            throw IOException("Path '${path}' does not refer to an existing file!")
        }
        val outputStream = ByteArrayOutputStream()
        val existingContent = this.getFileContentOrNull(path) ?: Bytes.EMPTY
        existingContent.writeToStream(outputStream)
        return ObservableOutputStream(outputStream){
            this.overwrite(path, Bytes(outputStream.toByteArray()))
        }
    }

    private class ObservableOutputStream(
        private val outputStream: OutputStream,
        private val onChange: () -> Unit
    ): OutputStream() {

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

        override fun write(b: Int) {
            this.outputStream.write(b)
        }

    }

}