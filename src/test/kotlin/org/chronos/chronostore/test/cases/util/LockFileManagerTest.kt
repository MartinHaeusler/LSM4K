package org.chronos.chronostore.test.cases.util

import com.google.common.util.concurrent.Uninterruptibles
import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.impl.LockFileManager
import org.chronos.chronostore.test.util.VFSMode
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import java.io.File
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class LockFileManagerTest {

    @Test
    fun canAcquireAndReleaseLockOnDisk() {
        VFSMode.ONDISK.withVFS { vfs ->
            val lockFile = vfs.file("test.lck")
            val manager = LockFileManager(lockFile)

            // locking once works
            expectThat(manager.lock()).isTrue()
            // locking again works as well but returns false
            expectThat(manager.lock()).isFalse()

            // unlocking should be fine
            expectThat(manager.unlock()).isTrue()

            // unlocking again is no problem but returns false
            expectThat(manager.unlock()).isFalse()
        }
    }

    @Test
    fun canAcquireAndReleaseLockInMemory() {
        VFSMode.INMEMORY.withVFS { vfs ->
            val lockFile = vfs.file("test.lck")
            val manager = LockFileManager(lockFile)

            // locking is not necessary, so it's always false
            expectThat(manager.lock()).isFalse()
            // ...even if we do it again
            expectThat(manager.lock()).isFalse()

            // unlocking should be fine, but always returns false
            expectThat(manager.unlock()).isFalse()
            // ... even if we do it again.
            expectThat(manager.unlock()).isFalse()
        }
    }

}