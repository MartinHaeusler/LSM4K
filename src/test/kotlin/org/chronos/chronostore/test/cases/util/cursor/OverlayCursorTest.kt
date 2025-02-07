package org.chronos.chronostore.test.cases.util.cursor

import org.chronos.chronostore.test.util.CursorTestUtils.cursorOn
import org.chronos.chronostore.test.util.DebugCursor.Companion.debug
import org.chronos.chronostore.util.ResourceContext.Companion.using
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.OverlayCursor.Companion.overlayOnto
import org.chronos.chronostore.util.cursor.OverlayCursor.Companion.overlayUnder
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.*

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

        overlayCursor.firstOrThrow()
        expect {
            that(overlayCursor.ascendingEntrySequenceFromHere().toList()).containsExactly(
                "a" to 5,
                "b" to 4,
                "c" to 5,
                "d" to 4,
                "s" to 5,
                "z" to 4,
            )
            that(cursor1.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(3)
                get(Cursor<*, *>::previous).isNull()
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
            that(cursor2.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(4)
                get(Cursor<*, *>::previous).isNull()
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
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

        // start at the first element
        overlayCursor.firstOrThrow()
        // iterate through the full sequence
        val resultList = overlayCursor.ascendingEntrySequenceFromHere().toList()

        expect {
            that(resultList).containsExactly(
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

            that(bigCursor.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(10)
                get(Cursor<*, *>::previous).isNull()
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
            that(smallCursor.callCounts) {
                getValue(Cursor<*, *>::first).isEqualTo(1)
                getValue(Cursor<*, *>::next).isEqualTo(2)
                get(Cursor<*, *>::previous).isNull()
                get(Cursor<*, *>::peekPrevious).isNull()
                get(Cursor<*, *>::peekNext).isNull()
                get(Cursor<*, *>::seekExactlyOrNext).isNull()
                get(Cursor<*, *>::seekExactlyOrPrevious).isNull()
            }
        }
    }

    private fun generateKeyValuePairs(amount: Int): List<Pair<String, String>> {
        require(amount > 0) { "Argument 'amount' must not be negative!" }
        val digits = (amount - 1).toString().length
        return (0..<amount).map { "Key-${it.toString().padStart(digits, '0')}" to "Value-${it.toString().padStart(digits, '0')}" }
    }

}