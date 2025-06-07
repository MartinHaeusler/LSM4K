package io.github.martinhaeusler.lsm4k.test.util.fakestoredsl

import io.github.martinhaeusler.lsm4k.util.FileIndex

@FakeStoreDSL
interface LevelOrTierConfigurator {

    fun file(index: FileIndex? = null, configure: LsmFileConfigurator.() -> Unit = {})

}