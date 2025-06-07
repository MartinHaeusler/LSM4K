package io.github.martinhaeusler.lsm4k.util.json

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.datatype.pcollections.PCollectionsModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.InputStream
import java.io.OutputStream

object JsonUtil {

    val OBJECT_MAPPER: ObjectMapper = ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        // serialize /  deserialize all known fields
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        // ignore getters and setters
        .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
        .setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE)
        // enable support for Kotlin classes
        .registerKotlinModule()
        // enable support for PCollection classes
        .registerModule(PCollectionsModule())
        // support for java.time.* classes (notably java.time.Duration)
        .registerModule(JavaTimeModule())
        // enable support for some miscellaneous classes
        .registerModule(LSM4KJacksonModule)

    fun writeJson(obj: Any, prettyPrint: Boolean = false): String {
        val writer = if (prettyPrint) {
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
        } else {
            OBJECT_MAPPER.writer()
        }
        return writer.writeValueAsString(obj)
    }

    fun writeJson(obj: Any, outputStream: OutputStream) {
        return OBJECT_MAPPER.writeValue(outputStream, obj)
    }

    fun readJsonAsNode(json: String): JsonNode {
        return OBJECT_MAPPER.readTree(json)
    }

    inline fun <reified T> readJsonAsObject(json: String): T {
        return OBJECT_MAPPER.readValue(json)
    }

    inline fun <reified T> readJsonAsObject(input: InputStream): T {
        return OBJECT_MAPPER.readValue(input)
    }

}