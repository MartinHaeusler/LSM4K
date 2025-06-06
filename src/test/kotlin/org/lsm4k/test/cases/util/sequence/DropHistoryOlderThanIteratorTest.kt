package org.lsm4k.test.cases.util.sequence

import org.junit.jupiter.api.Test
import org.lsm4k.model.command.Command
import org.lsm4k.util.iterator.IteratorExtensions.dropHistoryOlderThan
import org.lsm4k.util.iterator.IteratorExtensions.toList
import strikt.api.expectThat
import strikt.assertions.containsExactly

class DropHistoryOlderThanIteratorTest {

    @Test
    fun canDropHistoryOlderThanX() {
        val data = listOf(
            Command.put("a", 10, "a1"),
            Command.del("a", 20),
            Command.put("a", 30, "a3"),
            Command.put("b", 10, "b1"),
            Command.put("b", 25, "b2"),
            Command.put("b", 30, "b3"),
            Command.put("c", 10, "c1"),
            Command.put("d", 30, "d1"),
        )

        expectThat(data.iterator().dropHistoryOlderThan(25).toList()).containsExactly(
            Command.put("a", 30, "a3"),
            Command.put("b", 25, "b2"),
            Command.put("b", 30, "b3"),
            Command.put("c", 10, "c1"),
            Command.put("d", 30, "d1"),
        )
    }

}