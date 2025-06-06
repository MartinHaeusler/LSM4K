package org.lsm4k.test.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

@ParameterizedTest
@EnumSource(LSM4KMode::class)
@Target(AnnotationTarget.FUNCTION)
annotation class DatabaseEngineTest()
