package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest

class ChronoStoreStartStopTest {

    @ChronoStoreTest
    fun canOpenAndCloseChronoStore(mode: ChronoStoreMode) {
        mode.withChronoStore {
            // nothing to do here, we just want to make
            // sure that the store opened and closed correctly
        }
    }

}