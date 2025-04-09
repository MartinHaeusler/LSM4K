package org.chronos.chronostore.test.cases.util.cursor

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.test.util.CursorTestUtils.asString
import org.chronos.chronostore.test.util.CursorTestUtils.isEqualToKeyValuePair
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.test.util.junit.TestTags
import org.chronos.chronostore.test.util.lsm.LsmFileFactory.createLsmTreeFile
import org.chronos.chronostore.util.bytes.Bytes
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import strikt.api.expectThat
import strikt.assertions.*

@Tag(TestTags.UNIT_TEST)
class ChronoStoreFileCursorTest {

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canSeekFirstAndLastAndIterate(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val lsmFile = vfs.createLsmTreeFile(
                index = 0,
                content = listOf(
                    Command.put("a", 1, "a1"),
                    Command.put("a", 2, "a2"),
                    Command.put("a", 3, "a3"),
                    Command.put("c", 1, "c1"),
                    Command.put("c", 2, "c2"),
                    Command.put("c", 3, "c3"),
                )
            )
            lsmFile.cursor().use { cursor ->
                cursor.firstOrThrow()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@1")
                    get { this.value.asString() }.isEqualTo("a@1->a1")
                }

                cursor.lastOrThrow()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@3")
                    get { this.value.asString() }.isEqualTo("c@3->c3")
                }

                cursor.firstOrThrow()
                expectThat(cursor.ascendingEntrySequenceFromHere().toList()) {
                    hasSize(6)
                    get(0).isEqualToKeyValuePair("a@1", "a@1->a1")
                    get(1).isEqualToKeyValuePair("a@2", "a@2->a2")
                    get(2).isEqualToKeyValuePair("a@3", "a@3->a3")
                    get(3).isEqualToKeyValuePair("c@1", "c@1->c1")
                    get(4).isEqualToKeyValuePair("c@2", "c@2->c2")
                    get(5).isEqualToKeyValuePair("c@3", "c@3->c3")
                }

                cursor.lastOrThrow()
                expectThat(cursor.descendingEntrySequenceFromHere().toList()) {
                    hasSize(6)
                    get(5).isEqualToKeyValuePair("a@1", "a@1->a1")
                    get(4).isEqualToKeyValuePair("a@2", "a@2->a2")
                    get(3).isEqualToKeyValuePair("a@3", "a@3->a3")
                    get(2).isEqualToKeyValuePair("c@1", "c@1->c1")
                    get(1).isEqualToKeyValuePair("c@2", "c@2->c2")
                    get(0).isEqualToKeyValuePair("c@3", "c@3->c3")
                }
            }
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canSeekExactlyOrPrevious(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val lsmFile = vfs.createLsmTreeFile(
                index = 0,
                content = listOf(
                    Command.put("a", 1, "a1"),
                    Command.put("a", 2, "a2"),
                    Command.put("a", 3, "a3"),
                    Command.put("c", 1, "c1"),
                    Command.put("c", 2, "c2"),
                    Command.put("c", 3, "c3"),
                )
            )
            lsmFile.cursor().use { cursor ->
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of(""), 1))).isFalse()
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("a"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@1")
                    get { this.value.asString() }.isEqualTo("a@1->a1")
                }

                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("a"), 4))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@3")
                    get { this.value.asString() }.isEqualTo("a@3->a3")
                }

                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("b"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@3")
                    get { this.value.asString() }.isEqualTo("a@3->a3")
                }
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("c"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@1")
                    get { this.value.asString() }.isEqualTo("c@1->c1")
                }
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("c"), 3))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@3")
                    get { this.value.asString() }.isEqualTo("c@3->c3")
                }
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("c"), 4))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@3")
                    get { this.value.asString() }.isEqualTo("c@3->c3")
                }
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("d"), 4))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@3")
                    get { this.value.asString() }.isEqualTo("c@3->c3")
                }
            }
        }
    }


    @ParameterizedTest
    @VirtualFileSystemTest
    fun canSeekExactlyOrNext(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val lsmFile = vfs.createLsmTreeFile(
                index = 0,
                content = listOf(
                    Command.put("a", 1, "a1"),
                    Command.put("a", 2, "a2"),
                    Command.put("a", 3, "a3"),
                    Command.put("c", 1, "c1"),
                    Command.put("c", 2, "c2"),
                    Command.put("c", 3, "c3"),
                )
            )
            lsmFile.cursor().use { cursor ->
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of(""), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@1")
                    get { this.value.asString() }.isEqualTo("a@1->a1")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("a"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@1")
                    get { this.value.asString() }.isEqualTo("a@1->a1")
                }

                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("a"), 4))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@1")
                    get { this.value.asString() }.isEqualTo("c@1->c1")
                }

                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("c"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@1")
                    get { this.value.asString() }.isEqualTo("c@1->c1")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("c"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@1")
                    get { this.value.asString() }.isEqualTo("c@1->c1")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("c"), 3))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("c@3")
                    get { this.value.asString() }.isEqualTo("c@3->c3")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("c"), 4))).isFalse()
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("d"), 4))).isFalse()
            }
        }
    }
}