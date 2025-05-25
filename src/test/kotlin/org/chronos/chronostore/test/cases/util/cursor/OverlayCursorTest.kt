package org.chronos.chronostore.test.cases.util.cursor

import org.chronos.chronostore.test.util.CursorTestUtils.cursorOn
import org.chronos.chronostore.test.util.DebugCursor.Companion.debug
import org.chronos.chronostore.test.util.junit.UnitTest
import org.chronos.chronostore.util.ResourceContext.Companion.using
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.OverlayCursor.Companion.overlayOnto
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.*

@UnitTest
class OverlayCursorTest {

    @Test
    fun canOverlayTwoCursors(): Unit = using {
        val cursor1 = cursorOn(
            "a" to 5,
            "c" to 5,
            "s" to 5,
        ).debug("Cursor A")
            .autoClose()

        val cursor2 = cursorOn(
            "b" to 4,
            "c" to 4,
            "d" to 4,
            "z" to 4
        ).debug("Cursor B")
            .autoClose()

        val overlayCursor = cursor1.overlayOnto(cursor2)

        expect {
            that(overlayCursor.listAllEntriesAscending()).containsExactly(
                "a" to 5,
                "b" to 4,
                "c" to 5,
                "d" to 4,
                "s" to 5,
                "z" to 4,
            )
            that(overlayCursor.listAllEntriesDescending()).containsExactly(
                "z" to 4,
                "s" to 5,
                "d" to 4,
                "c" to 5,
                "b" to 4,
                "a" to 5,
            )
            that(cursor1.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(3)
                getValue(Cursor<*, *>::previous).isEqualTo(3)
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
            that(cursor2.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(4)
                getValue(Cursor<*, *>::previous).isEqualTo(4)
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
        }
    }

    @Test
    fun overlayDataWithEmpty() = using {
        val base = cursorOn(
            "a" to 1,
            "b" to 2,
            "z" to 26,
        ).autoClose()

        val overlay = cursorOn<String, Int>().autoClose()

        val cursor = overlay.overlayOnto(base).autoClose()

        expectThat(cursor) {
            get { this.listAllEntriesAscending() }.containsExactly("a" to 1, "b" to 2, "z" to 26)
        }
    }

    @Test
    fun overlayEmptyWithData() = using {
        val base = cursorOn<String, Int>().autoClose()

        val overlay = cursorOn(
            "a" to 1,
            "b" to 2,
            "z" to 26,
        ).autoClose()

        val cursor = overlay.overlayOnto(base).autoClose()

        expectThat(cursor) {
            get { this.listAllEntriesAscending() }.containsExactly("a" to 1, "b" to 2, "z" to 26)
        }
    }

    @Test
    fun overlayLargeCursorWithSmallCursor() = using {
        val bigCursor = cursorOn(
            *generateKeyValuePairs(10).toTypedArray()
        ).debug("Big Cursor")
            .autoClose()

        val smallCursor = cursorOn(
            "Key-3" to "foo",
            "Key-5" to "bar",
        ).debug("Small Cursor")
            .autoClose()

        val overlayCursor = smallCursor.overlayOnto(bigCursor)

        expect {
            that(overlayCursor.listAllEntriesAscending()).containsExactly(
                "Key-0" to "Value-0",
                "Key-1" to "Value-1",
                "Key-2" to "Value-2",
                "Key-3" to "foo",
                "Key-4" to "Value-4",
                "Key-5" to "bar",
                "Key-6" to "Value-6",
                "Key-7" to "Value-7",
                "Key-8" to "Value-8",
                "Key-9" to "Value-9",
            )

            that(overlayCursor.listAllEntriesDescending()).containsExactly(
                "Key-9" to "Value-9",
                "Key-8" to "Value-8",
                "Key-7" to "Value-7",
                "Key-6" to "Value-6",
                "Key-5" to "bar",
                "Key-4" to "Value-4",
                "Key-3" to "foo",
                "Key-2" to "Value-2",
                "Key-1" to "Value-1",
                "Key-0" to "Value-0",
            )

            that(bigCursor.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(10)
                get(Cursor<*, *>::previous).isEqualTo(10)
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
            that(smallCursor.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(2)
                get(Cursor<*, *>::previous).isEqualTo(2)
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
        }
    }

    @Test
    fun overlayCursorWithAllDeletions() = using {
        val baseCursor = cursorOn(
            "foo" to "bar",
            "hello" to "world",
            "lorem" to "ipsum",
        ).autoClose()

        val overlayCursor: Cursor<String, String?> = cursorOn(
            "foo" to null,
            "hello" to null,
            "lorem" to null,
        )

        val finalCursor = overlayCursor.overlayOnto(baseCursor).autoClose()

        expectThat(finalCursor) {
            get { this.listAllEntriesAscending() }.isEmpty()
            get { this.listAllEntriesDescending() }.isEmpty()
            get { this.first() }.isFalse()
            get { this.last() }.isFalse()
        }
    }


    @Test
    fun overlayCursorWithAllDeletionsAndSingleInsertions() = using {
        val baseCursor = cursorOn(
            "foo" to "bar",
            "hello" to "world",
            "lorem" to "ipsum",
        ).autoClose()

        val overlayCursor: Cursor<String, String?> = cursorOn(
            "foo" to null,
            "foo2" to "bar2",
            "hello" to null,
            "hello2" to "world2",
            "lorem" to null,
        )

        val finalCursor = overlayCursor.overlayOnto(baseCursor).autoClose()

        expectThat(finalCursor) {
            get { this.listAllEntriesAscending() }.containsExactly("foo2" to "bar2", "hello2" to "world2")
            get { this.listAllEntriesDescending() }.containsExactly("hello2" to "world2", "foo2" to "bar2")
            get { this.first() }.isTrue()
            get { this.last() }.isTrue()
        }
    }

    @Test
    fun overlayCursorWithAllDeletionsAndMultipleInsertions() = using {
        val baseCursor = cursorOn(
            "foo" to "bar",
            "hello" to "world",
            "lorem" to "ipsum",
        ).autoClose()

        val overlayCursor: Cursor<String, String?> = cursorOn(
            "foo" to null,
            "foo2" to "bar2",
            "foo3" to "bar3",
            "hello" to null,
            "hello2" to "world2",
            "hello3" to "world3",
            "lorem" to null,
        )

        val finalCursor = overlayCursor.overlayOnto(baseCursor).autoClose()

        expectThat(finalCursor) {
            get { this.listAllEntriesAscending() }.containsExactly("foo2" to "bar2", "foo3" to "bar3", "hello2" to "world2", "hello3" to "world3")
            get { this.listAllEntriesDescending() }.containsExactly("hello3" to "world3", "hello2" to "world2", "foo3" to "bar3", "foo2" to "bar2")
            get { this.firstEntryOrNull() }.isEqualTo("foo2" to "bar2")
            get { this.lastEntryOrNull() }.isEqualTo("hello3" to "world3")
        }
    }

    @Test
    fun failingFuzz1() = using {
        val baseCursor = cursorOn(
            "h" to "h-base",
            "l" to "l-base",
            "r" to "r-base",
            "t" to "t-base",
        ).autoClose()

        val overlayCursor = cursorOn(
            "b" to "b-overlay",
            "d" to "d-overlay",
            "f" to null,
            "h" to null,
            "j" to "j-overlay",
            "l" to null,
            "n" to "n-overlay",
            "p" to "p-overlay",
            "r" to null,
            "t" to null,
        ).autoClose()

        val cursor = overlayCursor.overlayOnto(baseCursor).autoClose()

        expectThat(cursor.listAllEntriesAscending()).containsExactly(
            "b" to "b-overlay",
            "d" to "d-overlay",
            "j" to "j-overlay",
            "n" to "n-overlay",
            "p" to "p-overlay",
        )

        Unit
    }

    private fun generateKeyValuePairs(amount: Int): List<Pair<String, String>> {
        require(amount > 0) { "Argument 'amount' must not be negative!" }
        val digits = (amount - 1).toString().length
        return (0..<amount).map { "Key-${it.toString().padStart(digits, '0')}" to "Value-${it.toString().padStart(digits, '0')}" }
    }

}