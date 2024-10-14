package org.chronos.chronostore.test.cases.util.cursor

import org.chronos.chronostore.util.cursor.IndexBasedCursor
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue

class IndexBasedCursorTest {

    @Test
    fun canIterateOverList() {
        val list = (0..9).map { it to it }
        IndexBasedCursor(
            minIndex = 0,
            maxIndex = list.lastIndex,
            getEntryAtIndex = list::get,
            name = "List Cursor"
        ).use { cursor ->
            cursor.firstOrThrow()
            val resultList = cursor.ascendingEntrySequenceFromHere().toList()
            expectThat(resultList).containsExactly(list)
        }
    }

    @Test
    fun canGetFirstAndLast() {
        val list = (0..9).map { it to it }
        IndexBasedCursor(
            minIndex = 0,
            maxIndex = list.lastIndex,
            getEntryAtIndex = list::get,
            name = "List Cursor"
        ).use { cursor ->
            repeat(5) {
                cursor.firstOrThrow()
                expectThat(cursor) {
                    get { this.keyOrNull }.isEqualTo(0)
                    get { this.valueOrNull }.isEqualTo(0)
                }
                cursor.lastOrThrow()
                expectThat(cursor) {
                    get { this.keyOrNull }.isEqualTo(9)
                    get { this.valueOrNull }.isEqualTo(9)
                }
            }
        }
    }

    @Test
    fun canGetExactlyOrNext() {
        val list = (0..9 step 2).map { it to it }
        IndexBasedCursor(
            minIndex = 0,
            maxIndex = list.lastIndex,
            getEntryAtIndex = list::get,
            name = "List Cursor"
        ).use { cursor ->
            repeat(5) {
                expectThat(cursor.seekExactlyOrNext(0)).isTrue()
                expectThat(cursor) {
                    get { this.keyOrNull }.isEqualTo(0)
                    get { this.valueOrNull }.isEqualTo(0)
                }
                expectThat(cursor.seekExactlyOrNext(-1)).isTrue()
                expectThat(cursor) {
                    get { this.keyOrNull }.isEqualTo(0)
                    get { this.valueOrNull }.isEqualTo(0)
                }
                expectThat(cursor.seekExactlyOrNext(1)).isTrue()
                expectThat(cursor) {
                    get { this.keyOrNull }.isEqualTo(2)
                    get { this.valueOrNull }.isEqualTo(2)
                }
                expectThat(cursor.seekExactlyOrNext(10)).isFalse()
            }
        }
    }

}