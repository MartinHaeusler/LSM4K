package org.chronos.chronostore.util.json

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.InputStream
import java.io.OutputStream

object JsonUtil {

    val OBJECT_MAPPER = ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerKotlinModule()

    fun writeJson(obj: Any): String {
        return OBJECT_MAPPER.writeValueAsString(obj)
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