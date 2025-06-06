package org.lsm4k.test.util.fakestoredsl

import org.lsm4k.util.FileIndex

@FakeStoreDSL
interface TierConfigurator {

    fun file(index: FileIndex? = null, configure: LsmFileConfigurator.() -> Unit)

}