package org.lsm4k.test.cases.util.cursor

import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.lsm4k.io.vfs.VirtualFileSystem
import org.lsm4k.lsm.LSMTreeFile
import org.lsm4k.model.command.Command
import org.lsm4k.model.command.KeyAndTSN
import org.lsm4k.test.util.CursorTestUtils.asString
import org.lsm4k.test.util.CursorTestUtils.isEqualToKeyValuePair
import org.lsm4k.test.util.VFSMode
import org.lsm4k.test.util.VirtualFileSystemTest
import org.lsm4k.test.util.junit.TestTags
import org.lsm4k.test.util.lsm.LsmFileFactory.createLsmTreeFile
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.cursor.LevelOrTierCursor
import org.lsm4k.util.statistics.StatisticsCollector
import org.lsm4k.util.statistics.StatisticsReporter
import strikt.api.expectThat
import strikt.assertions.*

@Tag(TestTags.UNIT_TEST)
class LevelOrTierCursorTest {

    private fun setUpExampleLsmFiles(vfs: VirtualFileSystem, statisticsReporter: StatisticsReporter): List<LSMTreeFile> {
        return listOf(
            vfs.createLsmTreeFile(
                statisticsReporter = statisticsReporter,
                index = 0,
                content = listOf(
                    Command.put("a", 1, "a1"),
                    Command.put("a", 2, "a2"),
                    Command.put("a", 3, "a3"),
                    Command.put("b", 1, "b1"),
                    Command.put("b", 2, "b2"),
                    Command.put("b", 3, "b3"),
                )
            ),
            vfs.createLsmTreeFile(
                statisticsReporter = statisticsReporter,
                index = 1,
                content = listOf(
                    Command.put("e", 1, "e1"),
                    Command.put("e", 2, "e2"),
                    Command.put("e", 3, "e3"),
                    Command.put("h", 1, "h1"),
                    Command.put("h", 2, "h2"),
                    Command.put("h", 3, "h3"),
                )
            ),
            vfs.createLsmTreeFile(
                statisticsReporter = statisticsReporter,
                index = 2,
                content = listOf(
                    Command.put("h", 10, "h10"),
                    Command.put("h", 20, "h20"),
                    Command.put("h", 30, "h30"),
                    Command.put("h", 40, "h40"),
                    Command.put("h", 50, "h50"),
                    Command.put("h", 60, "h60"),
                )
            ),
            vfs.createLsmTreeFile(
                statisticsReporter = statisticsReporter,
                index = 3,
                content = listOf(
                    Command.put("y", 1, "y1"),
                    Command.put("y", 2, "y2"),
                    Command.put("y", 3, "y3"),
                    Command.put("z", 1, "z1"),
                    Command.put("z", 2, "z2"),
                    Command.put("z", 3, "z3"),
                )
            ),
        )
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canMoveFirstAndLastAndIterate(vfsMode: VFSMode) {
        val stats = StatisticsCollector()
        vfsMode.withVFS { vfs ->
            val lsmFiles = setUpExampleLsmFiles(vfs, stats)
            LevelOrTierCursor(lsmFiles).use { cursor ->
                cursor.firstOrThrow()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@1")
                    get { this.value.asString() }.isEqualTo("a@1->a1")
                }

                cursor.lastOrThrow()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("z@3")
                    get { this.value.asString() }.isEqualTo("z@3->z3")
                }

                cursor.firstOrThrow()
                expectThat(cursor.ascendingEntrySequenceFromHere().toList()).describedAs("all entries (ASC)").and {
                    hasSize(24)
                    get(0).isEqualToKeyValuePair("a@1", "a@1->a1")
                    get(1).isEqualToKeyValuePair("a@2", "a@2->a2")
                    get(2).isEqualToKeyValuePair("a@3", "a@3->a3")
                    get(3).isEqualToKeyValuePair("b@1", "b@1->b1")
                    get(4).isEqualToKeyValuePair("b@2", "b@2->b2")
                    get(5).isEqualToKeyValuePair("b@3", "b@3->b3")

                    get(6).isEqualToKeyValuePair("e@1", "e@1->e1")
                    get(7).isEqualToKeyValuePair("e@2", "e@2->e2")
                    get(8).isEqualToKeyValuePair("e@3", "e@3->e3")
                    get(9).isEqualToKeyValuePair("h@1", "h@1->h1")
                    get(10).isEqualToKeyValuePair("h@2", "h@2->h2")
                    get(11).isEqualToKeyValuePair("h@3", "h@3->h3")

                    get(12).isEqualToKeyValuePair("h@10", "h@10->h10")
                    get(13).isEqualToKeyValuePair("h@20", "h@20->h20")
                    get(14).isEqualToKeyValuePair("h@30", "h@30->h30")
                    get(15).isEqualToKeyValuePair("h@40", "h@40->h40")
                    get(16).isEqualToKeyValuePair("h@50", "h@50->h50")
                    get(17).isEqualToKeyValuePair("h@60", "h@60->h60")

                    get(18).isEqualToKeyValuePair("y@1", "y@1->y1")
                    get(19).isEqualToKeyValuePair("y@2", "y@2->y2")
                    get(20).isEqualToKeyValuePair("y@3", "y@3->y3")
                    get(21).isEqualToKeyValuePair("z@1", "z@1->z1")
                    get(22).isEqualToKeyValuePair("z@2", "z@2->z2")
                    get(23).isEqualToKeyValuePair("z@3", "z@3->z3")
                }

                cursor.lastOrThrow()
                expectThat(cursor.descendingEntrySequenceFromHere().toList()).describedAs("all entries (DESC)").and {
                    hasSize(24)
                    get(23).isEqualToKeyValuePair("a@1", "a@1->a1")
                    get(22).isEqualToKeyValuePair("a@2", "a@2->a2")
                    get(21).isEqualToKeyValuePair("a@3", "a@3->a3")
                    get(20).isEqualToKeyValuePair("b@1", "b@1->b1")
                    get(19).isEqualToKeyValuePair("b@2", "b@2->b2")
                    get(18).isEqualToKeyValuePair("b@3", "b@3->b3")

                    get(17).isEqualToKeyValuePair("e@1", "e@1->e1")
                    get(16).isEqualToKeyValuePair("e@2", "e@2->e2")
                    get(15).isEqualToKeyValuePair("e@3", "e@3->e3")
                    get(14).isEqualToKeyValuePair("h@1", "h@1->h1")
                    get(13).isEqualToKeyValuePair("h@2", "h@2->h2")
                    get(12).isEqualToKeyValuePair("h@3", "h@3->h3")

                    get(11).isEqualToKeyValuePair("h@10", "h@10->h10")
                    get(10).isEqualToKeyValuePair("h@20", "h@20->h20")
                    get(9).isEqualToKeyValuePair("h@30", "h@30->h30")
                    get(8).isEqualToKeyValuePair("h@40", "h@40->h40")
                    get(7).isEqualToKeyValuePair("h@50", "h@50->h50")
                    get(6).isEqualToKeyValuePair("h@60", "h@60->h60")

                    get(5).isEqualToKeyValuePair("y@1", "y@1->y1")
                    get(4).isEqualToKeyValuePair("y@2", "y@2->y2")
                    get(3).isEqualToKeyValuePair("y@3", "y@3->y3")
                    get(2).isEqualToKeyValuePair("z@1", "z@1->z1")
                    get(1).isEqualToKeyValuePair("z@2", "z@2->z2")
                    get(0).isEqualToKeyValuePair("z@3", "z@3->z3")
                }
            }
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canSeekExactlyOrNext(vfsMode: VFSMode) {
        val stats = StatisticsCollector()
        vfsMode.withVFS { vfs ->
            val lsmFiles = setUpExampleLsmFiles(vfs, stats)
            LevelOrTierCursor(lsmFiles).use { cursor ->
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of(""), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("a@1")
                    get { this.value.asString() }.isEqualTo("a@1->a1")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("e"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("e@1")
                    get { this.value.asString() }.isEqualTo("e@1->e1")
                }

                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("c"), 21))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("e@1")
                    get { this.value.asString() }.isEqualTo("e@1->e1")
                }

                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("b"), 4))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("e@1")
                    get { this.value.asString() }.isEqualTo("e@1->e1")
                }

                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("b"), 3))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("b@3")
                    get { this.value.asString() }.isEqualTo("b@3->b3")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("h"), 61))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("y@1")
                    get { this.value.asString() }.isEqualTo("y@1->y1")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("z"), 3))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("z@3")
                    get { this.value.asString() }.isEqualTo("z@3->z3")
                }
                expectThat(cursor.seekExactlyOrNext(KeyAndTSN(Bytes.of("z"), 4))).isFalse()
            }
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canSeekExactlyOrPrevious(vfsMode: VFSMode) {
        val stats = StatisticsCollector()
        vfsMode.withVFS { vfs ->
            val lsmFiles = setUpExampleLsmFiles(vfs, stats)
            LevelOrTierCursor(lsmFiles).use { cursor ->
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of(""), 1))).isFalse()
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("e"), 1))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("e@1")
                    get { this.value.asString() }.isEqualTo("e@1->e1")
                }

                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("c"), 21))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("b@3")
                    get { this.value.asString() }.isEqualTo("b@3->b3")
                }

                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("b"), 4))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("b@3")
                    get { this.value.asString() }.isEqualTo("b@3->b3")
                }

                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("b"), 3))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("b@3")
                    get { this.value.asString() }.isEqualTo("b@3->b3")
                }
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("h"), 61))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("h@60")
                    get { this.value.asString() }.isEqualTo("h@60->h60")
                }
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("z"), 3))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("z@3")
                    get { this.value.asString() }.isEqualTo("z@3->z3")
                }
                expectThat(cursor.seekExactlyOrPrevious(KeyAndTSN(Bytes.of("z"), 4))).isTrue()
                expectThat(cursor) {
                    get { this.key.asString() }.isEqualTo("z@3")
                    get { this.value.asString() }.isEqualTo("z@3->z3")
                }
            }
        }
    }

}