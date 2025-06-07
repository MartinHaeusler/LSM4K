package io.github.martinhaeusler.lsm4k.async.taskmonitor

interface TaskMonitorListener {

    fun handleWorked(sourceMonitor: TaskMonitor, workDelta: Double) {}

    fun handleStarted(sourceMonitor: TaskMonitor, taskName: String) {}

    fun handleFailed(sourceMonitor: TaskMonitor, message: String, cause: Throwable?) {}

    fun handleDone(sourceMonitor: TaskMonitor) {}

}