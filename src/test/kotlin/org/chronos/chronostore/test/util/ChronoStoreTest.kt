package org.chronos.chronostore.test.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

@ParameterizedTest
@EnumSource(ChronoStoreMode::class)
@Target(AnnotationTarget.FUNCTION)
annotation class ChronoStoreTest()
