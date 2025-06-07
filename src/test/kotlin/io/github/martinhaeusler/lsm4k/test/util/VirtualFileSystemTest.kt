package io.github.martinhaeusler.lsm4k.test.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

@ParameterizedTest
@EnumSource(VFSMode::class)
@Target(AnnotationTarget.FUNCTION)
annotation class VirtualFileSystemTest {

}