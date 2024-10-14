package org.chronos.chronostore.test.cases.util.cursor

import org.chronos.chronostore.test.util.CursorTestUtils.cursorOn
import org.chronos.chronostore.test.util.DebugCursor.Companion.debug
import org.chronos.chronostore.util.ResourceContext.Companion.using
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.OverlayCursor
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

        val overlayCursor = OverlayCursor(
            base = cursor2,
            overlay = cursor1,
        ).autoClose()

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
            that(cursor1.callCounts){
                getValue(Cursor<*,*>::first).isEqualTo(1)
                getValue(Cursor<*,*>::next).isEqualTo(4)
                get(Cursor<*,*>::previous).isNull()
                get(Cursor<*,*>::peekPrevious).isNull()
                get(Cursor<*,*>::peekNext).isNull()
                get(Cursor<*,*>::seekExactlyOrNext).isNull()
                get(Cursor<*,*>::seekExactlyOrPrevious).isNull()
            }
            that(cursor2.callCounts){
                getValue(Cursor<*,*>::first).isEqualTo(1)
                getValue(Cursor<*,*>::next).isEqualTo(4)
                get(Cursor<*,*>::previous).isNull()
                get(Cursor<*,*>::peekPrevious).isNull()
                get(Cursor<*,*>::peekNext).isNull()
                get(Cursor<*,*>::seekExactlyOrNext).isNull()
                get(Cursor<*,*>::seekExactlyOrPrevious).isNull()
            }
        }

    }

}