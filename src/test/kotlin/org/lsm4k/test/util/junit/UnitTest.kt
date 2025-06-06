package org.lsm4k.test.util.junit

import org.junit.jupiter.api.Tag

@Tag(TestTags.UNIT_TEST)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class UnitTest
