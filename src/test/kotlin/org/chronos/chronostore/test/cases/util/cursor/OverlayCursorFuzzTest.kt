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

            // create the overlay maps
            val overlayMaps = mutableListOf<Map<String, String?>>()
            repeat(Random.nextInt(1..3)) {
                val overlayMapSize = Random.nextInt(0..allKeys.size)
                val overlayMap = allKeys.selectUniqueRandomSample(overlayMapSize).associateWith {
                    if (Random.nextBoolean()) {
                        "${it}-overlay"
                    } else {
                        null
                    }
                }
                overlayMaps += overlayMap
            }

            val resultMap = buildExpectedResultMap(baseMap, overlayMaps)

            try {
                OverlayCursorFuzzTestUtils.runOverlayCursorTestCase(
                    baseMap = baseMap,
                    overlayMaps = overlayMaps,
                    resultMap = resultMap,
                    allKeys = allKeys,
                    nonExistingKeys = nonExistingKeys,
                )
            } catch (t: Throwable) {
                println("A ${t::class.simpleName} occurred during a fuzz test:")
                println(t.stackTraceToString())

                Thread.sleep(100)
                val generatedSampleTestCode = generateSampleTestSourceCode(allKeys, nonExistingKeys, baseMap, overlayMaps, resultMap)

                fail(
                    "Test iteration #${iteration} failed due to a ${t::class.simpleName}." +
                        " Please refer to the logs for the exact reason and the test data.\n\n" +
                        "${testSetupToString(baseMap, overlayMaps)}\n\n" +
                        "GENERATED TEST CODE:\n\n" +
                        generatedSampleTestCode, t
                )
            }

        }

    }

    private fun generateSampleTestSourceCode(
        allKeys: Set<String>,
        nonExistingKeys: Set<String>,
        baseMap: Map<String, String>,
        overlayMaps: List<Map<String, String?>>,
        resultMap: NavigableMap<String, String>,
    ): String {

        val baseMapMembers = baseMap.entries.joinToString(
            prefix = "\n",
            separator = ",\n",
            postfix = ",\n",
        ) { "${it.key.toKotlinCode()} to ${it.value.toKotlinCode()}" }

        val overlayMapsCode = overlayMaps.joinToString(prefix = "listOf(", separator = ", ", postfix = ")") { overlayMap ->
            overlayMap.entries.joinToString(
                prefix = "\nmapOf(",
                separator = ",\n",
                postfix = ",\n),",
            ) { it.key.toKotlinCode() + " to " + it.value.toKotlinCode() }
        }


        val resultMapMembers = resultMap.entries.joinToString(
            prefix = "\n",
            separator = ",\n",
            postfix = ",\n"
        ) { it.key.toKotlinCode() + " to " + it.value.toKotlinCode() }

        val table = createOverviewTable(baseMap, overlayMaps, resultMap)

        val tableAsComment = table.toString().lines().joinToString(separator = "\n") { "//  ${it}" }

        return """
            @Test
            fun fuzzTestSample(){
            
                ${tableAsComment}
            
                val allKeys = setOf(${allKeys.joinToString(separator = ", ") { it.toKotlinCode() }})
                val nonExistingKeys = setOf(${nonExistingKeys.joinToString(separator = ", ") { it.toKotlinCode() }})
                val baseMap = mapOf(${baseMapMembers})
                val overlayMaps = $overlayMapsCode
                val resultMap = treeMapOf($resultMapMembers)
            
                runOverlayCursorTestCase(
                    baseMap = baseMap,
                    overlayMaps = overlayMaps,
                    resultMap = resultMap,
                    allKeys = allKeys,
                    nonExistingKeys = nonExistingKeys,
                )
            }
            """.trimIndent()
    }

    private fun createOverviewTable(
        baseMap: Map<String, String>,
        overlayMaps: List<Map<String, String?>>,
        resultMap: NavigableMap<String, String>,
    ): StringBuilder {
        val overviewMembers = mutableListOf<OverviewMember>()

        val allKeys = (baseMap.keys + overlayMaps.flatMap { it.keys }).distinct().sorted()

        for (key in allKeys) {
            val base = baseMap[key] ?: " "
            val overlays = overlayMaps.map { overlayMap ->
                if (overlayMap.contains(key)) {
                    overlayMap[key] ?: "<delete>"
                } else {
                    " "
                }
            }
            val result = resultMap[key] ?: "<skip>"
            overviewMembers += OverviewMember(key, base, overlays, result)
        }

        val keyColumnWidth = max("KEY".length, overviewMembers.maxOfOrNull { it.key.length } ?: 0)
        val baseColumnWidth = max("BASE".length, overviewMembers.maxOfOrNull { it.base.length } ?: 0)
        val overlayColumnWidths = overlayMaps.indices.map { index -> max("OVERLAY ${index}".length, overviewMembers.map { it.overlays[index] }.maxOfOrNull { it.length } ?: 0) }
        val resultColumnWidth = max("RESULT".length, overviewMembers.maxOfOrNull { it.result.length } ?: 0)

        val table = StringBuilder()
        table.append("KEY".padEnd(keyColumnWidth))
        table.append("   ")
        table.append("BASE".padEnd(baseColumnWidth))
        table.append("   ")
        overlayColumnWidths.mapIndexed { index, width ->
            table.append("OVERLAY ${index}".padEnd(width))
            table.append("   ")
        }
        table.append("RESULT".padEnd(resultColumnWidth))
        table.append("\n")
        for ((key, base, overlays, result) in overviewMembers) {
            table.append(key.padEnd(keyColumnWidth))
            table.append("   ")
            table.append(base.padEnd(baseColumnWidth))
            table.append("   ")
            for ((index, overlay) in overlays.withIndex()) {
                val width = overlayColumnWidths[index]
                table.append(overlay.padEnd(width))
                table.append("   ")
            }
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

    private fun testSetupToString(baseMap: Map<String, String>, overlayMaps: List<Map<String, String?>>): String {
        return """Fuzz test setup:
            |    Base Map: ${baseMap.entries.joinToString(prefix = "[", separator = ", ", postfix = "]") { "${it.key}: ${it.value}" }}
            |    ${
            overlayMaps.withIndex().joinToString(separator = "\n    ") { (index, overlayMap) ->
                "Overlay Map #${index}: ${overlayMap.entries.joinToString(prefix = "[", separator = ", ", postfix = "]") { "${it.key}: ${it.value}" }}"
            }
        }
        """.trimMargin()
    }

    private fun buildExpectedResultMap(
        baseMap: Map<String, String>,
        overlayMaps: List<Map<String, String?>>,
    ): NavigableMap<String, String> {
        val resultMap = TreeMap<String, String>()
        resultMap += baseMap
        for (overlayMap in overlayMaps) {
            for ((key, value) in overlayMap) {
                if (value == null) {
                    resultMap.remove(key)
                } else {
                    resultMap[key] = value
                }
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
        val overlays: List<String>,
        val result: String,
    )
}