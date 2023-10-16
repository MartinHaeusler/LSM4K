package org.chronos.chronostore.test.cases.util

import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.InverseQualifiedTemporalKey
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.bytes.BasicBytes
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import kotlin.random.Random

class InverseQualifiedTemporalKeyTest {


    @Test
    fun canSerializeAndDeserialize() {
        val storeId = StoreId.of("test")
        val userKey = Bytes.random(Random(System.currentTimeMillis()), 256)
        val original = InverseQualifiedTemporalKey(1234, storeId, userKey)
        val serialized = original.toBytes()
        val deserialized = InverseQualifiedTemporalKey.fromBytes(serialized)
        expectThat(deserialized).isEqualTo(original)
        val serializedAgain = deserialized.toBytes()
        expectThat(serializedAgain).isEqualTo(serialized)
    }

    @Test
    fun sortOrderBinaryAndInMemoryIsConsistent() {
        val storeId1 = StoreId.of("00000000-0000-0000-0000-000000000001")
        val storeId2 = StoreId.of("00000000-0000-0000-0000-000000000002")

        val userKey1 = BasicBytes("hello")
        val userKey2 = BasicBytes("world")

        val timestamp1 = 1234L
        val timestamp2 = 5678L

        val k1 = InverseQualifiedTemporalKey(timestamp1, storeId1, userKey1)
        val k2 = InverseQualifiedTemporalKey(timestamp1, storeId1, userKey2)
        val k3 = InverseQualifiedTemporalKey(timestamp1, storeId2, userKey1)
        val k4 = InverseQualifiedTemporalKey(timestamp1, storeId2, userKey2)
        val k5 = InverseQualifiedTemporalKey(timestamp2, storeId1, userKey1)
        val k6 = InverseQualifiedTemporalKey(timestamp2, storeId1, userKey2)
        val k7 = InverseQualifiedTemporalKey(timestamp2, storeId2, userKey1)
        val k8 = InverseQualifiedTemporalKey(timestamp2, storeId2, userKey2)

        val shuffled = listOf(k1,k2,k3,k4,k5,k6,k7,k8).shuffled()

        val binarySorted = shuffled
            .map(InverseQualifiedTemporalKey::toBytes)
            .sorted()
            .map(InverseQualifiedTemporalKey.Companion::fromBytes)
        val inMemorySorted = shuffled.sorted()

        expectThat(binarySorted).isEqualTo(inMemorySorted)
    }

}