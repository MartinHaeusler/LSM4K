package org.chronos.chronostore.test.cases.util.cursor

import org.chronos.chronostore.test.util.CursorTestUtils.cursorOn
import org.chronos.chronostore.util.ResourceContext
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.OverlayCursor.Companion.overlayOnto
import java.util.*

object OverlayCursorFuzzTestUtils {

    fun <K : Comparable<*>, V> treeMapOf(): TreeMap<K, V> {
        return TreeMap()
    }

    fun <K : Comparable<*>, V> treeMapOf(vararg entries: Pair<K, V>): TreeMap<K, V> {
        val map = TreeMap<K, V>()
        for (entry in entries) {
            map[entry.first] = entry.second
        }
        return map
    }


    @JvmStatic
    fun runOverlayCursorTestCase(
        baseMap: Map<String, String>,
        overlayMap: Map<String, String?>,
        resultMap: NavigableMap<String, String>,
        allKeys: Set<String>,
        nonExistingKeys: Set<String>,
    ) {
        ResourceContext.using {
            val base = cursorOn(baseMap.toList()).autoClose()
            val overlay = cursorOn(overlayMap.toList()).autoClose()
            val cursor = overlay.overlayOnto(base).autoClose()

            // get the first entry
            firstEntryTest(resultMap, cursor)

            // get the last entry
            lastEntryTest(resultMap, cursor)

            // get all keys ascending
            allKeysAscendingTest(resultMap, cursor)

            // get all keys descending
            allKeysDescendingTest(resultMap, cursor)

            // perform the following operation (this tests a cursor "turning around" at the corners):
            // - start at the first entry
            // - iterate upwards until the last (collecting entries)
            // - iterate backwards until the first (collecting entries)
            // - iterate backwards until the last (collecting entries)
            entriesAscendingDescendingAscendingTest(resultMap, cursor)

            // try to perform "exactOrNext" and "exactOrPrevious" with all keys existing in the map
            seekExactlyOrNeighborForExistingKeysTest(allKeys, resultMap, cursor)

            // try to perform "exactOrNext" and "exactOrPrevious" with keys absent from the map
            seekExactlyOrNeighborForAbsentKeysTest(nonExistingKeys, resultMap, cursor)
        }
    }

    private fun firstEntryTest(resultMap: NavigableMap<String, String>, cursor: Cursor<String, String>) {
        val expectedFirstEntry = resultMap.firstEntry()?.toPair()
        val actualFirstEntry = cursor.firstEntryOrNull()
        assertOperationResultEquals("firstEntryOrNull", expectedFirstEntry, actualFirstEntry)
    }

    private fun lastEntryTest(resultMap: NavigableMap<String, String>, cursor: Cursor<String, String>) {
        val expectedLastEntry = resultMap.lastEntry()?.toPair()
        val actualLastEntry = cursor.lastEntryOrNull()
        assertOperationResultEquals("lastEntryOrNull", expectedLastEntry, actualLastEntry)
    }

    private fun allKeysAscendingTest(resultMap: NavigableMap<String, String>, cursor: Cursor<String, String>) {
        val expectedListAllAscending = resultMap.entries.map { it.toPair() }
        val actualListAllAscending = cursor.listAllEntriesAscending()
        assertOperationResultEquals("listAllEntriesAscending", expectedListAllAscending, actualListAllAscending)
    }

    private fun allKeysDescendingTest(resultMap: NavigableMap<String, String>, cursor: Cursor<String, String>) {
        val expectedListAllDescending = resultMap.reversed().entries.map { it.toPair() }
        val actualListAllDescending = cursor.listAllEntriesDescending()
        assertOperationResultEquals("listAllEntriesDescending", expectedListAllDescending, actualListAllDescending)
    }

    private fun entriesAscendingDescendingAscendingTest(resultMap: NavigableMap<String, String>, cursor: Cursor<String, String>) {
        if (resultMap.isNotEmpty()) {
            cursor.firstOrThrow()
            val run1 = cursor.ascendingEntrySequenceFromHere(true).toList()
            val run2 = cursor.descendingEntrySequenceFromHere(true).toList()
            val run3 = cursor.ascendingEntrySequenceFromHere(true).toList()

            val actualResult = run1 + run2 + run3
            val expectedResult = resultMap.entries.map { it.toPair() } +
                resultMap.entries.reversed().map { it.toPair() } +
                resultMap.map { it.toPair() }

            assertOperationResultEquals("runUpAndDownAndUpAgain", expectedResult, actualResult)
        }
    }

    private fun seekExactlyOrNeighborForAbsentKeysTest(
        nonExistingKeys: Set<String>,
        resultMap: NavigableMap<String, String>,
        cursor: Cursor<String, String>,
    ) {
        for (nonKey in nonExistingKeys) {
            val expectedExactlyOrPrevious = resultMap.floorEntry(nonKey)?.toPair()
            val actualExactlyOrPrevious = cursor.exactOrPreviousEntry(nonKey)
            assertOperationResultEquals("exactOrPreviousEntry", expectedExactlyOrPrevious, actualExactlyOrPrevious)

            val expectedExactlyOrNext = resultMap.ceilingEntry(nonKey)?.toPair()
            val actualExactlyOrNext = cursor.exactOrNextEntry(nonKey)
            assertOperationResultEquals("exactOrNextEntry", expectedExactlyOrNext, actualExactlyOrNext)
        }
    }

    private fun seekExactlyOrNeighborForExistingKeysTest(
        allKeys: Set<String>,
        resultMap: NavigableMap<String, String>,
        cursor: Cursor<String, String>,
    ) {
        for (key in allKeys) {
            val expectedExactlyOrPrevious = resultMap.floorEntry(key)?.toPair()
            val actualExactlyOrPrevious = cursor.exactOrPreviousEntry(key)
            assertOperationResultEquals("exactOrPreviousEntry", expectedExactlyOrPrevious, actualExactlyOrPrevious)

            val expectedExactlyOrNext = resultMap.ceilingEntry(key)?.toPair()
            val actualExactlyOrNext = cursor.exactOrNextEntry(key)
            assertOperationResultEquals("exactOrNextEntry", expectedExactlyOrNext, actualExactlyOrNext)
        }
    }

    private fun assertOperationResultEquals(operationName: String, expectedResult: Any?, actualResult: Any?) {
        if (expectedResult != actualResult) {
            AssertionError("Operation '${operationName}' failed. Expected: ${expectedResult}, actual: ${actualResult}")
        }
    }

}