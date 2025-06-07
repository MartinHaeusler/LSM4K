package io.github.martinhaeusler.lsm4k.test.cases.api

import io.github.martinhaeusler.lsm4k.api.DatabaseEngine
import io.github.martinhaeusler.lsm4k.api.LSM4KConfiguration
import io.github.martinhaeusler.lsm4k.api.exceptions.FileMissingException
import io.github.martinhaeusler.lsm4k.test.extensions.DatabaseEngineTestExtensions.flushAllStoresSynchronous
import io.github.martinhaeusler.lsm4k.test.extensions.DatabaseEngineTestExtensions.flushStoreSynchronous
import io.github.martinhaeusler.lsm4k.test.extensions.transaction.LSM4KTransactionTestExtensions.put
import io.github.martinhaeusler.lsm4k.test.util.DatabaseEngineTest
import io.github.martinhaeusler.lsm4k.test.util.LSM4KMode
import io.github.martinhaeusler.lsm4k.test.util.VFSMode
import io.github.martinhaeusler.lsm4k.test.util.VirtualFileSystemTest
import io.github.martinhaeusler.lsm4k.util.StoreId
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*

class EngineStartStopTest {

    @DatabaseEngineTest
    fun canOpenAndCloseDatabaseEngine(mode: LSM4KMode) {
        mode.withDatabaseEngine {
            // nothing to do here, we just want to make
            // sure that the store opened and closed correctly
        }
    }

    @VirtualFileSystemTest
    fun canReopenEngineWithExistingData(mode: VFSMode) {
        mode.withVFS { vfs ->
            // open the database engine, add some data
            DatabaseEngine.openOnVirtualFileSystem(vfs, LSM4KConfiguration(compressionAlgorithm = "gzip")).use { engine ->
                engine.readWriteTransaction { tx ->
                    tx.createNewStore("foo")
                    tx.createNewStore("bar")
                    tx.createNewStore("empty")
                    tx.commit()
                }

                engine.readWriteTransaction { tx ->
                    val foo = tx.getStore("foo")
                    foo.put("a", "1")
                    foo.put("b", "2")
                    foo.put("c", "3")

                    val bar = tx.getStore("bar")
                    bar.put("x", "1")
                    bar.put("y", "2")
                    bar.put("z", "3")

                    tx.commit()
                }

                // flush "foo", leave "bar" and "empty" alone
                engine.flushStoreSynchronous("foo")
            }

            // reopen the database engine instance on the same data and check that
            // everything is still present
            DatabaseEngine.openOnVirtualFileSystem(vfs).use { engine ->
                engine.readWriteTransaction { tx ->
                    val allStores = tx.allStores.map { it.storeId }
                    expectThat(allStores).containsExactlyInAnyOrder(
                        StoreId.of("foo"),
                        StoreId.of("bar"),
                        StoreId.of("empty"),
                    )

                    expectThat(tx.getStore("foo").getEntriesAscending())
                        .map { it.first.asString() to it.second.asString() }
                        .containsExactly(
                            "a" to "1",
                            "b" to "2",
                            "c" to "3",
                        )

                    expectThat(tx.getStore("bar").getEntriesAscending())
                        .map { it.first.asString() to it.second.asString() }
                        .containsExactly(
                            "x" to "1",
                            "y" to "2",
                            "z" to "3",
                        )

                    expectThat(tx.getStore("empty").getEntriesAscending()).isEmpty()
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canDetectMissingLsmFilesDuringStartup(mode: VFSMode){
        mode.withVFS { vfs ->
            // open database engine, add some data
            DatabaseEngine.openOnVirtualFileSystem(vfs).use { engine ->
                engine.readWriteTransaction { tx ->
                    tx.createNewStore("foo")
                    tx.commit()
                }

                engine.readWriteTransaction { tx ->
                    val foo = tx.getStore("foo")
                    foo.put("a", "1")
                    foo.put("b", "2")
                    foo.put("c", "3")

                    tx.commit()
                }

                engine.flushAllStoresSynchronous()
            }

            // delete the file in "foo"
            vfs.directory("foo").file("0000000000.lsm").delete()

            // attempt to restart the database engine and make sure that we get an error
            expectThrows<FileMissingException> {
                DatabaseEngine.openOnVirtualFileSystem(vfs).close()
            }.and {
                get { this.message }.isNotNull().and {
                    contains("foo")
                    contains("0000000000.lsm")
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canDetectAndDeleteSuperfluousFilesDuringStartup(mode: VFSMode){
        mode.withVFS { vfs ->
            // open database engine, add some data
            DatabaseEngine.openOnVirtualFileSystem(vfs).use { engine ->
                engine.readWriteTransaction { tx ->
                    tx.createNewStore("foo")
                    tx.commit()
                }

                engine.readWriteTransaction { tx ->
                    val foo = tx.getStore("foo")
                    foo.put("a", "1")
                    foo.put("b", "2")
                    foo.put("c", "3")

                    tx.commit()
                }

                engine.flushAllStoresSynchronous()
            }

            // delete the file in "foo"
            vfs.directory("foo").file("0000000001.lsm").create()

            // attempt to restart the database engine and make sure that the extra file gets deleted
            DatabaseEngine.openOnVirtualFileSystem(vfs).close()

            expectThat(vfs.directory("foo").file("0000000001.lsm").exists()).isFalse()
        }
    }
}