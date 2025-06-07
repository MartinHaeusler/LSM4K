package io.github.martinhaeusler.lsm4k.async.taskmonitor

interface TaskMonitor {

    companion object {

        fun create(): TaskMonitor {
            return TaskMonitorImpl()
        }

        inline fun <T> TaskMonitor.mainTask(name: String, action: () -> T): T {
            this.reportStarted(name)
            try {
                return action()
            } catch (e: Exception) {
                this.reportFailed("Failed to execute task '${name}'! Cause: ${e}", e)
                throw e
            } finally {
                if (!this.status.state.isTerminal) {
                    this.reportDone()
                }
            }
        }

        inline fun <T> TaskMonitor.subTask(work: Double, name: String, action: () -> T): T {
            if (Thread.interrupted()) {
                throw InterruptedException()
            }
            return subMonitor(work).mainTask(name, action)
        }

        inline fun <T> TaskMonitor.subTaskWithMonitor(work: Double, action: (TaskMonitor) -> T): T {
            if (Thread.interrupted()) {
                throw InterruptedException()
            }
            val subMonitor = this.subMonitor(work)
            try {
                return action(subMonitor)
            } catch (e: Exception) {
                this.reportFailed(e)
                throw e
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun <T, R> TaskMonitor.map(work: Double, name: String, elements: List<T>, action: (T) -> R): List<R> {
            val resultList = ArrayList<R>(elements.size)
            this.forEach(work, name, elements) {
                resultList += action(it)
            }
            return resultList
        }

        inline fun <T, R> TaskMonitor.forEach(work: Double, name: String, elements: List<T>, action: (T) -> R) {
            if (this.status.state == State.INITIAL) {
                this.reportStarted(name)
            }
            if (elements.isEmpty()) {
                this.reportProgress(work)
                return
            }
            val subMonitor = this.subMonitor(work)
            subMonitor.reportStarted(name)
            val workPerItem = 1.0 / elements.size
            try {
                for (element in elements) {
                    if (Thread.interrupted()) {
                        throw InterruptedException()
                    }
                    action(element)
                    subMonitor.reportProgress(workPerItem)
                }
            } catch (e: Exception) {
                this.reportFailed(e)
                throw e
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun <T, R> TaskMonitor.map(work: Double, name: String, elements: List<T>, action: (TaskMonitor, T) -> R): List<R> {
            val resultList = ArrayList<R>(elements.size)
            this.forEachWithMonitor(work, name, elements) { monitor, element ->
                resultList += action(monitor, element)
            }
            return resultList
        }

        inline fun <T, R> TaskMonitor.forEachWithMonitor(work: Double, name: String, elements: List<T>, action: (TaskMonitor, T) -> R) {
            if (this.status.state == State.INITIAL) {
                this.reportStarted(name)
            }
            if (elements.isEmpty()) {
                this.reportProgress(work)
                return
            }
            val subMonitor = this.subMonitor(work)
            subMonitor.reportStarted(name)
            val workPerItem = 1.0 / elements.size
            try {
                for (element in elements) {
                    if (Thread.interrupted()) {
                        throw InterruptedException()
                    }
                    subMonitor.subTaskWithMonitor(workPerItem) { itemMonitor ->
                        action(itemMonitor, element)
                    }
                    subMonitor.reportProgress(workPerItem)
                }
            } catch (e: Exception) {
                this.reportFailed(e)
                throw e
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun TaskMonitor.onSuccess(crossinline action: () -> Unit): TaskMonitor {
            this.addListener(object : TaskMonitorListener {

                override fun handleDone(sourceMonitor: TaskMonitor) {
                    action()
                }

            })

            return this
        }

        inline fun TaskMonitor.onFailure(crossinline action: () -> Unit): TaskMonitor {
            this.addListener(object : TaskMonitorListener {

                override fun handleFailed(sourceMonitor: TaskMonitor, message: String, cause: Throwable?) {
                    action()
                }

            })

            return this
        }

        inline fun TaskMonitor.onExit(crossinline action: () -> Unit): TaskMonitor {
            this.addListener(object : TaskMonitorListener {
                private var called = false

                override fun handleDone(sourceMonitor: TaskMonitor) {
                    if (this.called) {
                        return
                    }
                    this.called = true
                    action()
                }

                override fun handleFailed(sourceMonitor: TaskMonitor, message: String, cause: Throwable?) {
                    if (this.called) {
                        return
                    }
                    this.called = true
                    action()
                }

            })

            return this
        }

    }

    fun reportStarted(taskName: String)

    fun reportProgress(worked: Double)

    fun reportDone()

    fun reportFailed(throwable: Throwable)

    fun reportFailed(message: String)

    fun reportFailed(message: String, throwable: Throwable)

    fun subMonitor(allocatedWork: Double): TaskMonitor

    fun addListener(taskMonitorListener: TaskMonitorListener)

    fun removeListener(taskMonitorListener: TaskMonitorListener)

    val status: Status

    enum class State {

        INITIAL {

            override val isTerminal: Boolean
                get() = false

        },

        IN_PROGRESS {

            override val isTerminal: Boolean
                get() = false

        },

        FAILED {

            override val isTerminal: Boolean
                get() = true

        },

        SUCCEEDED {

            override val isTerminal: Boolean
                get() = true

        },

        ;


        abstract val isTerminal: Boolean

    }

    class Status(
        val state: State,
        val progress: Double,
        val taskNames: List<String>,
        val failureMessage: String?,
        val failureCause: Throwable?,
    )

}