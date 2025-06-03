package org.chronos.chronostore.compressor.api

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.compressor.api.Compressors.getCompressorNames
import java.util.*

/**
 * Utilities for accessing the registered [Compressor] implementations.
 */
object Compressors {

    private val log = KotlinLogging.logger {}

    /** Simple cache for the compressors to avoid loading them twice (they're stateless, after all). */
    private val COMPRESSORS_BY_NAME: Map<String, Compressor>

    init {
        val serviceLoader = ServiceLoader.load(Compressor::class.java, Compressors::class.java.classLoader)
        val compressorsByName = mutableMapOf<String, Compressor>()
        for (compressor in serviceLoader) {
            val name = compressor.uniqueName
            log.trace { "Loading compressor '${name}' from class '${compressor::class.qualifiedName}'..." }
            if (name in compressorsByName) {
                throw IllegalStateException("Found multiple Compressor implementations with name '${name}'!")
            }
            compressorsByName[name] = compressor
        }
        log.debug { "Successfully loaded ${compressorsByName.size} Compressor implementation(s): ${compressorsByName.values.sortedBy { it.uniqueName }.joinToString { it.uniqueName }}" }
        COMPRESSORS_BY_NAME = Collections.unmodifiableMap(compressorsByName)
    }

    /**
     * Returns the set of all registered [Compressor] names.
     */
    fun getCompressorNames(): Set<String> {
        return COMPRESSORS_BY_NAME.keys
    }

    /**
     * Gets the compressor with the given (exact) [compressorName], or returns `null` if there is no such compressor.
     *
     * @param compressorName The (exact) [Compressor.uniqueName] of the [Compressor] to get.
     *
     * @return The compressor with the given name, or `null` if there is no such compressor.
     *
     * @see getCompressorNames
     */
    fun getCompressorForNameOrNull(compressorName: String): Compressor? {
        return COMPRESSORS_BY_NAME[compressorName]
    }

    /**
     * Gets the compressor with the given (exact) [compressorName].
     *
     * @param compressorName The (exact) [Compressor.uniqueName] of the [Compressor] to get.
     *
     * @return The compressor with the given name
     *
     * @see getCompressorNames
     *
     * @throws IllegalArgumentException if there is no registered compressor for the given [compressorName].
     *                                  If you're confident that the name is correct, please ensure that you
     *                                  have the corresponding library dependency in your build.
     *
     */
    fun getCompressorForName(compressorName: String): Compressor {
        return this.getCompressorForNameOrNull(compressorName)
            ?: throw IllegalArgumentException(
                "Could not find Compressor for name '${compressorName}'!" +
                    " Is the corresponding dependency installed?"
            )
    }

}