package org.chronos.chronostore.api

import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.lsm.merge.strategy.DefaultMergeService
import org.chronos.chronostore.lsm.merge.strategy.MergeService

enum class MergeStrategy {

    DEFAULT {

        override fun createMergeService(taskManager: AsyncTaskManager): MergeService {
            return DefaultMergeService(taskManager)
        }

    },

    ;

    abstract fun createMergeService(taskManager: AsyncTaskManager): MergeService

}