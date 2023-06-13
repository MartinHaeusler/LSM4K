package org.chronos.chronostore.test.util

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.iterator.IteratorExtensions.checkOrdered
import org.chronos.chronostore.util.iterator.IteratorExtensions.latestVersionOnly
import org.chronos.chronostore.util.iterator.IteratorExtensions.orderedDistinct
import org.chronos.chronostore.util.iterator.IteratorExtensions.toList
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.containsExactly

class LatestVersionOnlyIteratorTest {

    @Test
    fun willReportLatestVersionsOnly() {
        val commands = listOf(
            Command.put("hello", 100, "1"),
            Command.put("hello", 200, "2"),
            Command.put("hello", 300, "3"),
            Command.del("hello", 350),
            Command.put("hello", 400, "4"),
            Command.put("hello", 500, "5"),
            Command.put("hello", 600, "6"),
            Command.put("foo", 1, "bar"),
            Command.put("bar", 1, "baz"),
            Command.del("bar", 2),
        ).sorted()

        val latestCommands = commands.iterator()
            .checkOrdered()
            .orderedDistinct()
            .latestVersionOnly()
            .toList()

        expectThat(latestCommands).containsExactly(
            Command.del("bar", 2),
            Command.put("foo", 1, "bar"),
            Command.put("hello", 600, "6"),
        )

    }

}