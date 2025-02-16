package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.util.FileIndex

@FakeStoreDSL
interface TierConfigurator {

    fun file(index: FileIndex? = null, configure: LsmFileConfigurator.() -> Unit)

}