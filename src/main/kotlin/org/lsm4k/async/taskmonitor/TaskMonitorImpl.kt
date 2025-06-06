package org.lsm4k.async.taskmonitor

import org.lsm4k.async.taskmonitor.TaskMonitor.State

class TaskMonitorImpl : TaskMonitor {

    private var taskName: String? = null
    private var state = State.INITIAL
    private var failureMessage: String? = null
    private var failureCause: Throwable? = null

    private var localProgress: Double = 0.0

    private var currentSubTaskMonitor: TaskMonitorImpl? = null

    private val listeners = mutableListOf<TaskMonitorListener>()


    override val status: TaskMonitor.Status
        get() = TaskMonitor.Status(
            state = this.state,
            progress = this.localProgress,
            taskNames = getTaskNames(),
            failureMessage = this.failureMessage,
            failureCause = this.failureCause,
        )

    override fun addListener(taskMonitorListener: TaskMonitorListener) {
        this.listeners += taskMonitorListener
    }

    override fun removeListener(taskMonitorListener: TaskMonitorListener) {
        this.listeners -= taskMonitorListener
    }

    override fun reportStarted(taskName: String) {
        check(this.state == State.INITIAL) { "Method is only allowed in INITIAL state. Monitor is in state ${this.state} with task name '${this.taskName}'." }
        this.taskName = taskName
        this.state = State.IN_PROGRESS
        this.fireStartedEvent(taskName)
    }

    override fun reportProgress(worked: Double) {
        if (this.state != State.IN_PROGRESS) {
            // Ignore progress reports when we're not IN_PROGRESS.
            // This can occur e.g. if a task has failed already.
            return
        }
        if (worked <= 0.0) {
            return
        }
        val progressBefore = this.localProgress
        this.localProgress = (this.localProgress + worked).coerceIn(0.0, 1.0)
        val progressAfter = this.localProgress
        val delta = (progressAfter - progressBefore)
        this.fireProgressEvent(delta)
    }

    override fun reportDone() {
        if (this.state.isTerminal) {
            // do not switch from FAILED to SUCCESS
            return
        }
        // report any missing work
        if (this.localProgress < 1.0) {
            this.reportProgress(1.0 - this.localProgress)
        }
        this.localProgress = 1.0
        this.state = State.SUCCEEDED
        this.fireDoneEvent()
    }

    override fun reportFailed(throwable: Throwable) {
        this.reportFailedInternal(throwable.toString(), throwable)
    }

    override fun reportFailed(message: String) {
        this.reportFailedInternal(message, null)
    }

    override fun reportFailed(message: String, throwable: Throwable) {
        this.reportFailedInternal(message, throwable)
    }

    private fun reportFailedInternal(message: String, throwable: Throwable?) {
        if (this.state == State.FAILED) {
            // already reported as failure
            return
        }
        this.state = State.FAILED
        this.failureMessage = message
        this.failureCause = throwable
        this.fireFailedEvent(message, throwable)
    }

    override fun subMonitor(allocatedWork: Double): TaskMonitor {
        check(this.state == State.IN_PROGRESS) {
            "Can only create subMonitor while the parent is IN_PROGRESS, but the parent is in state ${this.state}!"
        }
        check(this.currentSubTaskMonitor == null) {
            "Cannot create subMonitor - the current monitor already has a an active subMonitor. It needs to complete before a new one can be created!"
        }
        val subMonitor = TaskMonitorImpl()
        subMonitor.addListener(SubTaskMonitorListener(allocatedWork))
        this.currentSubTaskMonitor = subMonitor
        return subMonitor
    }


    private fun fireProgressEvent(workDelta: Double) {
        for (listener in this.listeners) {
            listener.handleWorked(this, workDelta)
        }
    }

    private fun fireStartedEvent(taskName: String) {
        for (listener in this.listeners) {
            listener.handleStarted(this, taskName)
        }
    }

    private fun fireFailedEvent(message: String, cause: Throwable?) {
        for (listener in this.listeners) {
            listener.handleFailed(this, message, cause)
        }
    }

    private fun fireDoneEvent() {
        for (listener in this.listeners) {
            listener.handleDone(this)
        }
    }

    private fun getTaskNames(): List<String> {
        val childTaskNames = this.currentSubTaskMonitor?.getTaskNames() ?: emptyList()
        val myTaskName = this.taskName
        return if (myTaskName.isNullOrBlank()) {
            childTaskNames
        } else {
            listOf(myTaskName) + childTaskNames
        }
    }

    private inner class SubTaskMonitorListener(
        val allocatedWorkInParent: Double
    ) : TaskMonitorListener {

        private var workReportedToParent: Double = 0.0

        override fun handleWorked(sourceMonitor: TaskMonitor, workDelta: Double) {
            val parent = this@TaskMonitorImpl

            if (this.workReportedToParent >= this.allocatedWorkInParent) {
                // we must not exceed the work allocated to us in the parent
                return
            }
            val availableWorkToReport = this.allocatedWorkInParent - this.workReportedToParent

            val workToReportToParent = (workDelta * this.allocatedWorkInParent).coerceIn(0.0, availableWorkToReport)
            if (workToReportToParent <= 0) {
                return
            }
            parent.reportProgress(workToReportToParent)
            this.workReportedToParent += workToReportToParent
        }

        override fun handleStarted(sourceMonitor: TaskMonitor, taskName: String) {
            // nothing to do
        }

        override fun handleFailed(sourceMonitor: TaskMonitor, message: String, cause: Throwable?) {
            val parent = this@TaskMonitorImpl
            parent.currentSubTaskMonitor = null
            parent.reportFailedInternal(message, cause)
        }

        override fun handleDone(sourceMonitor: TaskMonitor) {
            val parent = this@TaskMonitorImpl
            if (this.workReportedToParent < this.allocatedWorkInParent) {
                // report the remaining work
                this.handleWorked(sourceMonitor, (this.allocatedWorkInParent - this.workReportedToParent))
            }

            parent.currentSubTaskMonitor = null
        }

    }

}