package org.lsm4k.test.cases.util.cursor

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.lsm4k.util.cursor.NavigableMapCursor
import java.util.*
import kotlin.random.Random

class NavigableMapCursorTest {

    @Test
    fun canPerformOperationsOnNavigableMapCursor() {
        val map = TreeMap<Int, Int>()
        repeat(10_000) { map[it] = it * 2 }

        val random = Random(System.currentTimeMillis())

        NavigableMapCursor(map).use { cursor ->
            repeat(100_000){
                val action = random.nextInt(6)
                val currentEntry = if (cursor.isValidPosition) {
                    cursor.key to cursor.value
                } else {
                    null
                }

                when (action) {
                    0 -> {
                        // next
                        val expectedNext = if (currentEntry == null) {
                            null
                        } else {
                            map.higherEntry(currentEntry.first)?.toPair()
                        }
                        val nextResult = cursor.next()
                        if (currentEntry == null) {
                            // cursor is invalid state, moving should not be allowed
                            Assertions.assertFalse(nextResult)
                        } else {
                            if (expectedNext != null) {
                                Assertions.assertTrue(nextResult)
                                val actualNext = cursor.key to cursor.value
                                Assertions.assertEquals(expectedNext.first, actualNext.first)
                                Assertions.assertEquals(expectedNext.second, actualNext.second)
                            } else {
                                // map has no next entry
                                Assertions.assertFalse(nextResult)
                            }
                        }
                    }

                    1 -> {
                        // previous
                        val expectedPrevious = if (currentEntry == null) {
                            null
                        } else {
                            map.lowerEntry(currentEntry.first)?.toPair()
                        }
                        val previousResult = cursor.previous()
                        if (currentEntry == null) {
                            // cursor is invalid state, moving should not be allowed
                            Assertions.assertFalse(previousResult)
                        } else {
                            if (expectedPrevious != null) {
                                Assertions.assertTrue(previousResult)
                                val actualPrevious = cursor.key to cursor.value
                                Assertions.assertEquals(expectedPrevious.first, actualPrevious.first)
                                Assertions.assertEquals(expectedPrevious.second, actualPrevious.second)
                            } else {
                                // map has no previous entry
                                Assertions.assertFalse(previousResult)
                            }
                        }
                    }

                    2 -> {
                        // first
                        val expectedEntry = map.firstEntry().toPair()
                        Assertions.assertTrue(cursor.first())
                        val actualEntry = cursor.key to cursor.value
                        Assertions.assertEquals(expectedEntry.first, actualEntry.first)
                        Assertions.assertEquals(expectedEntry.second, actualEntry.second)
                    }

                    3 -> {
                        // last
                        val expectedEntry = map.lastEntry().toPair()
                        Assertions.assertTrue(cursor.last())
                        val actualEntry = cursor.key to cursor.value
                        Assertions.assertEquals(expectedEntry.first, actualEntry.first)
                        Assertions.assertEquals(expectedEntry.second, actualEntry.second)
                    }

                    4 -> {
                        // next higher
                        val target = random.nextInt(10_000)
                        val expectedEntry = map.ceilingEntry(target)?.toPair()
                        val seekSuccessful = cursor.seekExactlyOrNext(target)
                        if (expectedEntry == null) {
                            Assertions.assertFalse(seekSuccessful)
                        } else {
                            val actualEntry = cursor.key to cursor.value
                            Assertions.assertEquals(expectedEntry.first, actualEntry.first)
                            Assertions.assertEquals(expectedEntry.second, actualEntry.second)
                        }
                    }

                    5 -> {
                        // next lower
                        val target = random.nextInt(10_000)
                        val expectedEntry = map.floorEntry(target)?.toPair()
                        val seekSuccessful = cursor.seekExactlyOrPrevious(target)
                        if (expectedEntry == null) {
                            Assertions.assertFalse(seekSuccessful)
                        } else {
                            val actualEntry = cursor.key to cursor.value
                            Assertions.assertEquals(expectedEntry.first, actualEntry.first)
                            Assertions.assertEquals(expectedEntry.second, actualEntry.second)
                        }
                    }
                }
            }
        }


    }


}