package org.chronos.chronostore.util.json

import com.fasterxml.jackson.databind.deser.std.StdDelegatingDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer
import com.fasterxml.jackson.databind.util.StdConverter
import java.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration
import java.time.Duration as JavaDuration
import kotlin.time.Duration as KotlinDuration

object ChronoStoreJacksonModule : SimpleModule() {

    init {
        this.addSerializer(KotlinDuration::class.java, KotlinToJavaDurationConverter.delegatingSerializer)
        this.addDeserializer(KotlinDuration::class.java, JavaToKotlinDurationConverter.delegatingDeserializer)
    }


    internal object KotlinToJavaDurationConverter : StdConverter<KotlinDuration, JavaDuration>() {

        override fun convert(value: KotlinDuration): Duration {
            return value.toJavaDuration()
        }

        val delegatingSerializer: StdDelegatingSerializer by lazy {
            StdDelegatingSerializer(this)
        }

    }

    internal object JavaToKotlinDurationConverter : StdConverter<JavaDuration, KotlinDuration>() {

        override fun convert(value: JavaDuration): kotlin.time.Duration {
            return value.toKotlinDuration()
        }

        val delegatingDeserializer: StdDelegatingDeserializer<KotlinDuration> by lazy {
            StdDelegatingDeserializer(this)
        }
    }


}