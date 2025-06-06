package org.lsm4k.test.cases.io.vfs

import org.junit.jupiter.api.Assumptions
import org.lsm4k.io.fileaccess.InMemoryFileDriver
import org.lsm4k.io.fileaccess.MemoryMappedFileDriver
import org.lsm4k.io.fileaccess.MemorySegmentFileDriver
import org.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import org.lsm4k.io.fileaccess.RandomFileAccessDriverFactory.Companion.withDriver
import org.lsm4k.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.lsm4k.test.util.VFSMode
import org.lsm4k.test.util.VirtualFileSystemTest
import org.lsm4k.util.IOExtensions.withInputStream
import org.lsm4k.util.bytes.BasicBytes
import org.lsm4k.util.bytes.Bytes.Companion.writeBytesWithoutSize
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.EOFException
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

    @VirtualFileSystemTest
    fun canTruncateFile(mode: VFSMode) {
        mode.withVFS { vfs ->
            val file = vfs.file("test.txt")
            file.append { out ->
                out.writeBytesWithoutSize(BasicBytes("lorem ipsum dolor sit amet"))
            }
            file.truncateAfter(11)

            expectThat(file).get { this.length }.isEqualTo(11)

            val content = file.withInputStream { input -> input.readAllBytes() }
            expectThat(String(content)).isEqualTo("lorem ipsum")
        }
    }

    @VirtualFileSystemTest
    fun canUseInMemoryRandomFileAccessDriver(mode: VFSMode) {
        val driverFactory = InMemoryFileDriver.Factory
        runRandomFileAccessDriverTest(mode, driverFactory)
    }

    @VirtualFileSystemTest
    fun canUseMemoryMappedRandomFileAccessDriver(mode: VFSMode) {
        val driverFactory = MemoryMappedFileDriver.Factory
        runRandomFileAccessDriverTest(mode, driverFactory)
    }

    @VirtualFileSystemTest
    fun canUseMemorySegmentRandomFileAccessDriver(mode: VFSMode) {
        Assumptions.assumeTrue(MemorySegmentFileDriver.Factory.isAvailable) {
            "The MemorySegment API is not available for this run." +
                " Please attach the JVM option \"--add-modules jdk.incubator.foreign\" to enable it (JDK 17+ required)."
        }
        val driverFactory = MemorySegmentFileDriver.Factory
        runRandomFileAccessDriverTest(mode, driverFactory)
    }

    private fun runRandomFileAccessDriverTest(mode: VFSMode, driverFactory: RandomFileAccessDriverFactory) {
        mode.withVFS { vfs ->
            val meta = vfs.directory("meta")
            meta.mkdirs()
            val file = meta.file("myFile.txt")
            file.withOverWriter { overWriter ->
                overWriter.outputStream.write("Hello World".toByteArray())
                overWriter.commit()
            }
            driverFactory.withDriver(file) { driver ->
                expectThat(driver) {
                    get { this.fileSize }.isEqualTo(11)
                    get { this.readBytesOrNull(6, 5)?.asString() }.isNotNull().contains("World")
                }
                expectThrows<EOFException> {
                    // let's attempt to read more bytes than we need. This should fail.
                    driver.readBytes(0, 100)
                }
                // the following should not throw an exception though (note the "orNull")
                expectThat(driver){
                    get { this.readBytesOrNull(0, 100) }.isNull()
                }
            }
        }
    }
}