package org.chronos.chronostore.test.cases.util.cursor

import org.chronos.chronostore.test.util.junit.UnitTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.util.*
import kotlin.math.max
import kotlin.random.Random
import kotlin.random.nextInt

@UnitTest
class OverlayCursorFuzzTest {

    @Test
    fun fuzzTest() {
        val allKeys = setOf("b", "d", "f", "h", "j", "l", "n", "p", "r", "t")
        val nonExistingKeys = setOf("a", "c", "e", "g", "i", "k", "l", "m", "o", "q", "s", "u")

        repeat(100_000) { iteration ->
            if (iteration % 100 == 0) {
                println("Iteration #${iteration}")
            }
            // create a base map
            val baseMapSize = Random.nextInt(0..allKeys.size)
            val baseMap = allKeys.selectUniqueRandomSample(baseMapSize).associateWith {
                "${it}-base"
            }

            val overlayMapSize = Random.nextInt(0..allKeys.size)
            val overlayMap = allKeys.selectUniqueRandomSample(overlayMapSize).associateWith {
                if (Random.nextBoolean()) {
                    "${it}-overlay"
                } else {
                    null
                }
            }

            val resultMap = buildExpectedResultMap(baseMap, overlayMap)

            try {
                OverlayCursorFuzzTestUtils.runOverlayCursorTestCase(
                    baseMap = baseMap,
                    overlayMap = overlayMap,
                    resultMap = resultMap,
                    allKeys = allKeys,
                    nonExistingKeys = nonExistingKeys,
                )
            } catch (t: Throwable) {
                println("A ${t::class.simpleName} occurred during a fuzz test:")
                println(t.stackTraceToString())

                printTestSetup(baseMap, overlayMap)

                // generate the code for the stand-alone test case
                println()
                println()
                println("GENERATED TEST CODE:")
                println()
                println()
                println(generateSampleTestSourceCode(allKeys, nonExistingKeys, baseMap, overlayMap, resultMap))
                println()
                println()

                // sleep a little to avoid mixing up the logs (err vs. out)
                Thread.sleep(100)

                fail("Test iteration #${iteration} failed. Please refer to the logs for the exact reason and the test data.")
            }

        }

    }

    private fun generateSampleTestSourceCode(
        allKeys: Set<String>,
        nonExistingKeys: Set<String>,
        baseMap: Map<String, String>,
        overlayMap: Map<String, String?>,
        resultMap: NavigableMap<String, String>,
    ): String {

        val baseMapMembers = baseMap.entries.joinToString(
            prefix = "\n",
            separator = ",\n",
            postfix = ",\n",
        ) { "${it.key.toKotlinCode()} to ${it.value.toKotlinCode()}" }

        val overlayMapMembers = overlayMap.entries.joinToString(
            prefix = "\n",
            separator = ",\n",
            postfix = ",\n"
        ) { it.key.toKotlinCode() + " to " + it.value.toKotlinCode() }

        val resultMapMembers = resultMap.entries.joinToString(
            prefix = "\n",
            separator = ",\n",
            postfix = ",\n"
        ) { it.key.toKotlinCode() + " to " + it.value.toKotlinCode() }

        val table = createOverviewTable(baseMap, overlayMap, resultMap)

        val tableAsComment = table.toString().lines().joinToString(separator = "\n") { "//  ${it}" }

        return """
            @Test
            fun fuzzTestSample(){
            
                ${tableAsComment}
            
                val allKeys = setOf(${allKeys.joinToString(separator = ", ") { it.toKotlinCode() }})
                val nonExistingKeys = setOf(${nonExistingKeys.joinToString(separator = ", ") { it.toKotlinCode() }})
                val baseMap = mapOf(${baseMapMembers})
                val overlayMap = mapOf($overlayMapMembers)
                val resultMap = treeMapOf($resultMapMembers)
            
                runOverlayCursorTestCase(
                    baseMap = baseMap,
                    overlayMap = overlayMap,
                    resultMap = resultMap,
                    allKeys = allKeys,
                    nonExistingKeys = nonExistingKeys,
                )
            }
            """.trimIndent()
    }

    private fun createOverviewTable(
        baseMap: Map<String, String>,
        overlayMap: Map<String, String?>,
        resultMap: NavigableMap<String, String>,
    ): StringBuilder {
        val overviewMembers = mutableListOf<OverviewMember>()

        for (key in (baseMap.keys + overlayMap.keys).sorted()) {
            val base = baseMap[key] ?: " "
            val overlay = if (overlayMap.contains(key)) {
                overlayMap[key] ?: "<delete>"
            } else {
                " "
            }
            val result = resultMap[key] ?: "<skip>"
            overviewMembers += OverviewMember(key, base, overlay, result)
        }

        val keyColumnWidth = max("KEY".length, overviewMembers.maxOfOrNull { it.key.length } ?: 0)
        val baseColumnWidth = max("BASE".length, overviewMembers.maxOfOrNull { it.base.length } ?: 0)
        val overlayColumnWidth = max("OVERLAY".length, overviewMembers.maxOfOrNull { it.overlay.length } ?: 0)
        val resultColumnWidth = max("RESULT".length, overviewMembers.maxOfOrNull { it.result.length } ?: 0)

        val table = StringBuilder()
        table.append("KEY".padEnd(keyColumnWidth))
        table.append("   ")
        table.append("BASE".padEnd(baseColumnWidth))
        table.append("   ")
        table.append("OVERLAY".padEnd(overlayColumnWidth))
        table.append("   ")
        table.append("RESULT".padEnd(resultColumnWidth))
        table.append("\n")
        for ((key, base, overlay, result) in overviewMembers) {
            table.append(key.padEnd(keyColumnWidth))
            table.append("   ")
            table.append(base.padEnd(baseColumnWidth))
            table.append("   ")
            table.append(overlay.padEnd(overlayColumnWidth))
            table.append("   ")
            table.append(result.padEnd(resultColumnWidth))
            table.append("\n")
        }
        return table
    }

    private fun String?.toKotlinCode(quoteCharacter: Char = '"'): String {
        return if (this == null) {
            "null"
        } else {
            "${quoteCharacter}${this}${quoteCharacter}"
        }
    }

    private fun printTestSetup(baseMap: Map<String, String>, overlayMap: Map<String, String?>) {
        println()
        println()
        println("Fuzz test setup:")
        println("Base Map: ${baseMap.entries.joinToString(prefix = "[", separator = ", ", postfix = "]") { "${it.key}: ${it.value}" }}")
        println("Overlay Map: ${overlayMap.entries.joinToString(prefix = "[", separator = ", ", postfix = "]") { "${it.key}: ${it.value}" }}")
    }

    private fun buildExpectedResultMap(
        baseMap: Map<String, String>,
        overlayMap: Map<String, String?>,
    ): NavigableMap<String, String> {
        val resultMap = TreeMap<String, String>()
        resultMap += baseMap
        for ((key, value) in overlayMap) {
            if (value == null) {
                resultMap.remove(key)
            } else {
                resultMap[key] = value
            }
        }

        return resultMap
    }

    private fun <T> Set<T>.selectUniqueRandomSample(size: Int): Set<T> {
        require(this.isNotEmpty()) { "Cannot draw random samples from empty list!" }
        require(size in 0..this.size) { "Sample size (${size}) is out of bounds [0..${this.size}]!" }
        val remainingOptions = this.toMutableSet()

        val resultSet = mutableSetOf<T>()
        while (resultSet.size < size) {
            val element = remainingOptions.random()
            remainingOptions -= element
            resultSet += element
        }
        return resultSet
    }

    data class OverviewMember(
        val key: String,
        val base: String,
        val overlay: String,
        val result: String,
    )
}