package org.chronos.chronostore.test.cases.util

import org.chronos.chronostore.api.SystemStore
import org.chronos.chronostore.test.util.junit.UnitTest
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.json.JsonUtil
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*

@UnitTest
class StoreIdTest {

    @Test
    fun canCreateSystemStoreIds() {
        val storeIds = SystemStore.entries.map { it.storeId }
        expectThat(storeIds) {
            get { this.size }.isEqualTo(SystemStore.entries.size)
            get { this.distinct().size }.isEqualTo(SystemStore.entries.size)
        }
    }

    @Test
    fun canCreateBasicStoreId() {
        expect {
            that(StoreId.of("hello-world")).get { this.path }.containsExactly("hello-world")
            that(StoreId.of("hello_world")).get { this.path }.containsExactly("hello_world")
            that(StoreId.of("foo")).get { this.path }.containsExactly("foo")
        }
    }

    @Test
    fun canCreateStoreIdFromSegmentsContainingSlashes() {
        expect {
            that(StoreId.of("hello", "world")).get { this.path }.containsExactly("hello", "world")
            that(StoreId.of("hello/world")).get { this.path }.containsExactly("hello", "world")
            that(StoreId.of("foo/bar", "baz")).get { this.path }.containsExactly("foo", "bar", "baz")
        }
    }

    @Test
    fun cannotCreateStoreIdWithUppercaseCharacters() {
        expectThrows<IllegalArgumentException> {
            StoreId.of("helloWorld")
        }
    }

    @Test
    fun canCreateNestedStoreId() {
        expect {
            that(StoreId.of("foo")).get { this.path }.containsExactly("foo")
            that(StoreId.of("foo/bar")).get { this.path }.containsExactly("foo", "bar")
            that(StoreId.of("foo/bar/baz")).get { this.path }.contains("foo", "bar", "baz")
            that(StoreId.of("main/0/data")).get { this.path }.containsExactly("main", "0", "data")
        }
    }

    @Test
    fun cannotCreateStoreWithEmptyPathSegments() {
        expectThrows<IllegalArgumentException> {
            StoreId.of("foo//bar")
        }
        expectThrows<IllegalArgumentException> {
            StoreId.of("")
        }
        expectThrows<IllegalArgumentException> {
            StoreId.of("/")
        }
    }

    @Test
    fun cannotCreateStoreIdContainingWhitespace() {
        expectThrows<IllegalArgumentException> {
            StoreId.of("foo bar")
        }
    }

    @Test
    fun cannotCreateStoreIdContainingReservedNames() {
        expectThrows<IllegalArgumentException> {
            StoreId.of("nul")
        }
        expectThrows<IllegalArgumentException> {
            StoreId.of("hello/nul/world")
        }
        expectThrows<IllegalArgumentException> {
            StoreId.of("foo/prn")
        }
    }

    @Test
    fun canSerializeAndDeserializeStoreIds() {
        expect {
            that(StoreId.of("main/0/data")) {
                get { toBytes() }.and {
                    hasSize(11 /* characters */)
                    get { this.asString() }.isEqualTo("main/0/data")
                    get { StoreId.readFrom(this) }.get { this.path }.containsExactly("main", "0", "data")
                }
            }
        }
    }

    @Test
    fun toStringWorks() {
        expect {
            that(StoreId.of("main", "0", "data")).get { this.toString() }.isEqualTo("main/0/data")
            that(StoreId.of("foo")).get { this.toString() }.isEqualTo("foo")
        }
    }

    @Test
    fun hashCodeAndEqualsWork() {
        expect {
            that(StoreId.of("foo")) {
                isEqualTo(StoreId.of("foo"))
                get { this.hashCode() }.isEqualTo(StoreId.of("foo").hashCode())
            }
            that(StoreId.of("foo", "bar")) {
                isEqualTo(StoreId.of("foo/bar"))
                get { this.hashCode() }.isEqualTo(StoreId.of("foo/bar").hashCode())
            }
            that(StoreId.of("foo")) {
                isNotEqualTo(StoreId.of("bar"))
                get { this.hashCode() }.isNotEqualTo(StoreId.of("bar").hashCode())
            }
        }
    }

    @Test
    fun canSerializeAndDeserializeStoreIdWithJackson() {
        val foo = StoreId.of("foo")
        val fooBarBaz = StoreId.of("foo/bar/baz")

        expect {
            that(JsonUtil.writeJson(foo)).isEqualTo("\"foo\"")
            that(JsonUtil.writeJson(fooBarBaz)).isEqualTo("\"foo/bar/baz\"")
            that(JsonUtil.readJsonAsObject<StoreId>("\"foo\"")).isEqualTo(foo)
            that(JsonUtil.readJsonAsObject<StoreId>("\"foo/bar/baz\"")).isEqualTo(fooBarBaz)
        }
    }
}