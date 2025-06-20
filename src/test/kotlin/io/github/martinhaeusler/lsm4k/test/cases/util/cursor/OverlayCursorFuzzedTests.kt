package io.github.martinhaeusler.lsm4k.test.cases.util.cursor

import io.github.martinhaeusler.lsm4k.test.cases.util.cursor.OverlayCursorFuzzTestUtils.runOverlayCursorTestCase
import io.github.martinhaeusler.lsm4k.test.util.junit.UnitTest
import io.github.martinhaeusler.lsm4k.util.collection.TreeMapUtils.treeMapOf
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsCollector
import org.junit.jupiter.api.Test

/**
 * This test class contains tests produced by failing instances of [OverlayCursorFuzzTest].
 */
@UnitTest
class OverlayCursorFuzzedTests {

    @Test
    fun fuzzTestSample0() {

        //  KEY   BASE     OVERLAY     RESULT
        //  d     d-base               d-base
        //  f     null     <delete>    <skip>
        //  h     null     h-overlay   h-overlay
        //  j     null     j-overlay   j-overlay
        //  l     l-base               l-base
        //  n     n-base               n-base
        //

        val allKeys = setOf("b", "d", "f", "h", "j", "l", "n", "p", "r", "t")
        val nonExistingKeys = setOf("a", "c", "e", "g", "i", "k", "l", "m", "o", "q", "s", "u")
        val baseMap = mapOf(
            "l" to "l-base",
            "n" to "n-base",
            "d" to "d-base",
        )
        val overlayMap = mapOf(
            "h" to "h-overlay",
            "f" to null,
            "j" to "j-overlay",
        )
        val resultMap = treeMapOf(
            "d" to "d-base",
            "h" to "h-overlay",
            "j" to "j-overlay",
            "l" to "l-base",
            "n" to "n-base",
        )

        runOverlayCursorTestCase(
            baseMap = baseMap,
            overlayMap = overlayMap,
            resultMap = resultMap,
            allKeys = allKeys,
            nonExistingKeys = nonExistingKeys,
            statisticsReporter = StatisticsCollector(),
        )
    }

    @Test
    fun fuzzTestSample1() {

        //  KEY   BASE     OVERLAY     RESULT
        //  b              b-overlay   b-overlay
        //  d     d-base               d-base
        //  h     h-base   h-overlay   h-overlay
        //  j              <delete>    <skip>
        //  l     l-base   l-overlay   l-overlay
        //  n     n-base   <delete>    <skip>
        //  p              <delete>    <skip>
        //  r     r-base               r-base
        //

        val allKeys = setOf("b", "d", "f", "h", "j", "l", "n", "p", "r", "t")
        val nonExistingKeys = setOf("a", "c", "e", "g", "i", "k", "l", "m", "o", "q", "s", "u")
        val baseMap = mapOf(
            "d" to "d-base",
            "r" to "r-base",
            "h" to "h-base",
            "l" to "l-base",
            "n" to "n-base",
        )
        val overlayMap = mapOf(
            "l" to "l-overlay",
            "b" to "b-overlay",
            "h" to "h-overlay",
            "n" to null,
            "j" to null,
            "p" to null,
        )
        val resultMap = treeMapOf(
            "b" to "b-overlay",
            "d" to "d-base",
            "h" to "h-overlay",
            "l" to "l-overlay",
            "r" to "r-base",
        )

        runOverlayCursorTestCase(
            baseMap = baseMap,
            overlayMap = overlayMap,
            resultMap = resultMap,
            allKeys = allKeys,
            nonExistingKeys = nonExistingKeys,
            statisticsReporter = StatisticsCollector(),
        )
    }

    @Test
    fun fuzzTestSample2() {

        //  KEY   BASE     OVERLAY     RESULT
        //  b              b-overlay   b-overlay
        //  d     d-base   <delete>    <skip>
        //  f     f-base   <delete>    <skip>
        //  h              h-overlay   h-overlay
        //  j              <delete>    <skip>
        //  l     l-base   <delete>    <skip>
        //  n              <delete>    <skip>
        //  p     p-base   <delete>    <skip>
        //  r              r-overlay   r-overlay
        //

        val allKeys = setOf("b", "d", "f", "h", "j", "l", "n", "p", "r", "t")
        val nonExistingKeys = setOf("a", "c", "e", "g", "i", "k", "l", "m", "o", "q", "s", "u")
        val baseMap = mapOf(
            "d" to "d-base",
            "f" to "f-base",
            "p" to "p-base",
            "l" to "l-base",
        )
        val overlayMap = mapOf(
            "f" to null,
            "j" to null,
            "d" to null,
            "h" to "h-overlay",
            "b" to "b-overlay",
            "l" to null,
            "r" to "r-overlay",
            "p" to null,
            "n" to null,
        )
        val resultMap = treeMapOf(
            "b" to "b-overlay",
            "h" to "h-overlay",
            "r" to "r-overlay",
        )

        runOverlayCursorTestCase(
            baseMap = baseMap,
            overlayMap = overlayMap,
            resultMap = resultMap,
            allKeys = allKeys,
            nonExistingKeys = nonExistingKeys,
            statisticsReporter = StatisticsCollector(),
        )
    }

    @Test
    fun fuzzTestSample3() {

        //  KEY   BASE     OVERLAY     RESULT
        //  d              d-overlay   d-overlay
        //  f              <delete>    <skip>
        //  h              h-overlay   h-overlay
        //  j     j-base               j-base
        //  r              <delete>    <skip>
        //

        val allKeys = setOf("b", "d", "f", "h", "j", "l", "n", "p", "r", "t")
        val nonExistingKeys = setOf("a", "c", "e", "g", "i", "k", "l", "m", "o", "q", "s", "u")
        val baseMap = mapOf(
            "j" to "j-base",
        )
        val overlayMap = mapOf(
            "f" to null,
            "r" to null,
            "d" to "d-overlay",
            "h" to "h-overlay",
        )
        val resultMap = treeMapOf(
            "d" to "d-overlay",
            "h" to "h-overlay",
            "j" to "j-base",
        )

        runOverlayCursorTestCase(
            baseMap = baseMap,
            overlayMap = overlayMap,
            resultMap = resultMap,
            allKeys = allKeys,
            nonExistingKeys = nonExistingKeys,
            statisticsReporter = StatisticsCollector(),
        )
    }

    @Test
    fun fuzzTestSample4() {

        //  KEY   BASE     OVERLAY     RESULT
        //  b     b-base   <delete>    <skip>
        //  d     d-base               d-base
        //  h     h-base   <delete>    <skip>
        //  j     j-base   j-overlay   j-overlay
        //  n              n-overlay   n-overlay
        //  p              <delete>    <skip>
        //  t              t-overlay   t-overlay
        //

        val allKeys = setOf("b", "d", "f", "h", "j", "l", "n", "p", "r", "t")
        val nonExistingKeys = setOf("a", "c", "e", "g", "i", "k", "l", "m", "o", "q", "s", "u")
        val baseMap = mapOf(
            "j" to "j-base",
            "d" to "d-base",
            "h" to "h-base",
            "b" to "b-base",
        )
        val overlayMap = mapOf(
            "t" to "t-overlay",
            "h" to null,
            "n" to "n-overlay",
            "j" to "j-overlay",
            "b" to null,
            "p" to null,
        )
        val resultMap = treeMapOf(
            "d" to "d-base",
            "j" to "j-overlay",
            "n" to "n-overlay",
            "t" to "t-overlay",
        )

        runOverlayCursorTestCase(
            baseMap = baseMap,
            overlayMap = overlayMap,
            resultMap = resultMap,
            allKeys = allKeys,
            nonExistingKeys = nonExistingKeys,
            statisticsReporter = StatisticsCollector(),
        )
    }

}