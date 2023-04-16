package org.chronos.chronostore.async.taskmonitor

interface TaskMonitor {

    fun reportStarted(taskName: String)

    fun reportProgress(worked: Double)

    fun reportDone()

    fun reportFailed(throwable: Throwable)

    fun reportFailed(message: String)

    fun reportFailed(message: String, throwable: Throwable)

    fun subMonitor(allocatedWork: Double)


}