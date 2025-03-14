package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.exceptions.FileMissingException
import org.chronos.chronostore.test.extensions.ChronoStoreTestExtensions.flushStoreSynchronous
import org.chronos.chronostore.test.extensions.ChronoStoreTestExtensions.flushAllStoresSynchronous
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.put
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.StoreId
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*

class ChronoStoreStartStopTest {

    @ChronoStoreTest
    fun canOpenAndCloseChronoStore(mode: ChronoStoreMode) {
        mode.withChronoStore {
            // nothing to do here, we just want to make
            // sure that the store opened and closed correctly
        }
    }

    @VirtualFileSystemTest
    fun canReopenStoreWithExistingData(mode: VFSMode) {
        mode.withVFS { vfs ->
            // open chronostore, add some data
            ChronoStore.openOnVirtualFileSystem(vfs).use { chronoStore ->
                chronoStore.transaction { tx ->
                    tx.createNewStore("foo")
                    tx.createNewStore("bar")
                    tx.createNewStore("empty")
                    tx.commit()
                }

                chronoStore.transaction { tx ->
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
                chronoStore.flushStoreSynchronous("foo")
            }

            // reopen the chronostore instance on the same data and check that
            // everything is still present
            ChronoStore.openOnVirtualFileSystem(vfs).use { chronoStore ->
                chronoStore.transaction { tx ->
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
            // open chronostore, add some data
            ChronoStore.openOnVirtualFileSystem(vfs).use { chronoStore ->
                chronoStore.transaction { tx ->
                    tx.createNewStore("foo")
                    tx.commit()
                }

                chronoStore.transaction { tx ->
                    val foo = tx.getStore("foo")
                    foo.put("a", "1")
                    foo.put("b", "2")
                    foo.put("c", "3")

                    tx.commit()
                }

                chronoStore.flushAllStoresSynchronous()
            }

            // delete the file in "foo"
            vfs.directory("foo").file("0000000000.chronostore").delete()

            // attempt to restart chronostore and make sure that we get an error
            expectThrows<FileMissingException> {
                ChronoStore.openOnVirtualFileSystem(vfs).close()
            }.and {
                get { this.message }.isNotNull().and {
                    contains("foo")
                    contains("0000000000.chronostore")
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canDetectAndDeleteSuperfluousFilesDuringStartup(mode: VFSMode){
        mode.withVFS { vfs ->
            // open chronostore, add some data
            ChronoStore.openOnVirtualFileSystem(vfs).use { chronoStore ->
                chronoStore.transaction { tx ->
                    tx.createNewStore("foo")
                    tx.commit()
                }

                chronoStore.transaction { tx ->
                    val foo = tx.getStore("foo")
                    foo.put("a", "1")
                    foo.put("b", "2")
                    foo.put("c", "3")

                    tx.commit()
                }

                chronoStore.flushAllStoresSynchronous()
            }

            // delete the file in "foo"
            vfs.directory("foo").file("0000000001.chronostore").create()

            // attempt to restart chronostore and make sure that the extra file gets deleted
            ChronoStore.openOnVirtualFileSystem(vfs).close()

            expectThat(vfs.directory("foo").file("0000000001.chronostore").exists()).isFalse()
        }
    }
}