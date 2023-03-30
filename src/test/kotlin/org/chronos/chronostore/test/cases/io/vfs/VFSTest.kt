package org.chronos.chronostore.test.cases.io.vfs

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.IOExtensions.withInputStream
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.IOException

class VFSTest {

    @VirtualFileSystemTest
    fun canCreateWriteReadDeleteFile(mode: VFSMode) {
        mode.withVFS { vfs ->
            val file = vfs.file("test.txt")
            file.create()
            file.withOverWriter { overWriter ->
                overWriter.outputStream.write("Hello World".toByteArray())
                overWriter.commit()
            }
            expectThat(file) {
                get { exists() }.isTrue()
                get { this.length }.isEqualTo(11)
            }
            file.withInputStream {
                val text = String(it.readAllBytes())
                expectThat(text).isEqualTo("Hello World")
            }
            file.delete()
            expectThat(file) {
                get { exists() }.isFalse()
                get { this.length }.isEqualTo(0)
            }
        }
    }

    @VirtualFileSystemTest
    fun canCreateListDeleteFolder(mode: VFSMode) {
        mode.withVFS { vfs ->
            val fooDir = vfs.directory("foo")
            expectThat(fooDir) {
                get { exists() }.isFalse()
            }
            fooDir.mkdirs()
            expectThat(vfs.directory("foo")) {
                get { exists() }.isTrue()
            }
            val barTxt = fooDir.file("bar.txt")
            barTxt.create()
            val bazTxt = fooDir.file("baz.txt")
            bazTxt.create()
            expectThat(fooDir) {
                get { this.list() }.containsExactlyInAnyOrder("bar.txt", "baz.txt")
            }
            bazTxt.delete()
            expectThat(fooDir) {
                get { this.list() }.containsExactlyInAnyOrder("bar.txt")
            }
            fooDir.clear()
            expectThat(barTxt) {
                get { this.exists() }.isFalse()
            }
            expectThat(fooDir) {
                get { this.list() }.isEmpty()
            }
            fooDir.delete()
            expectThat(fooDir) {
                get { this.exists() }.isFalse()
                get { this.list() }.isEmpty()
            }
        }
    }

    @VirtualFileSystemTest
    fun canCreateDeleteNestedFolder(mode: VFSMode) {
        mode.withVFS { vfs ->
            val main = vfs.directory("main")
            val sub = main.directory("sub")
            val sub1 = sub.directory("sub1")
            val sub2 = sub.directory("sub2")

            sub1.mkdirs()
            sub2.mkdirs()

            expect {
                that(main) {
                    get { exists() }.isTrue()
                    get { list() }.containsExactly("sub")
                }
                that(sub) {
                    get { exists() }.isTrue()
                    get { list() }.containsExactlyInAnyOrder("sub1", "sub2")
                }
                that(sub1) {
                    get { exists() }.isTrue()
                }
                that(sub2) {
                    get { exists() }.isTrue()
                }
            }

            sub2.delete()
            expect {
                that(sub2).get { exists() }.isFalse()
                that(sub).get { list() }.containsExactly("sub1")
            }

            expectThrows<IOException> {
                sub.delete() // folder is not empty!
            }

            sub.clear()
            expectThat(sub) {
                get { exists() }.isTrue()
                get { list() }.isEmpty()
            }

            sub.delete()
            expect {
                that(sub).get { exists() }.isFalse()
                that(main).get { list() }.isEmpty()
            }
            main.delete()
            expectThat(main).get { exists() }.isFalse()
        }
    }

}