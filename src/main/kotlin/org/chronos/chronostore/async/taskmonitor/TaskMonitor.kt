package org.chronos.chronostore.async.taskmonitor

interface TaskMonitor {

    companion object {

        fun create(): TaskMonitor {
            return TaskMonitorImpl()
        }


        inline fun <T> TaskMonitor.subTask(work: Double, name: String, action: () -> T): T {
            val subMonitor = this.subMonitor(work)
            subMonitor.reportStarted(name)
            try {
                return action()
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun <T> TaskMonitor.subTask(work: Double, action: (TaskMonitor) -> T): T {
            val subMonitor = this.subMonitor(work)
            try {
                return action(subMonitor)
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun <T, R> TaskMonitor.map(work: Double, name: String, elements: List<T>, action: (T) -> R): List<R> {
            if (elements.isEmpty()) {
                this.reportProgress(work)
                return emptyList()
            }
            val subMonitor = this.subMonitor(work)
            subMonitor.reportStarted(name)
            val workPerItem = 1.0 / elements.size
            try {
                val results = mutableListOf<R>()
                for (element in elements) {
                    results += action(element)
                    subMonitor.reportProgress(workPerItem)
                }
                return results
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun <T, R> TaskMonitor.forEach(work: Double, name: String, elements: List<T>, action: (T) -> R) {
            if (elements.isEmpty()) {
                this.reportProgress(work)
                return
            }
            val subMonitor = this.subMonitor(work)
            subMonitor.reportStarted(name)
            val workPerItem = 1.0 / elements.size
            try {
                for (element in elements) {
                    action(element)
                    subMonitor.reportProgress(workPerItem)
                }
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun <T, R> TaskMonitor.map(work: Double, name: String, elements: List<T>, action: (TaskMonitor, T) -> R): List<R> {
            if (elements.isEmpty()) {
                this.reportProgress(work)
                return emptyList()
            }
            val subMonitor = this.subMonitor(work)
            subMonitor.reportStarted(name)
            val workPerItem = 1.0 / elements.size
            try {
                val results = mutableListOf<R>()
                for (element in elements) {
                    subMonitor.subTask(workPerItem) { itemMonitor ->
                        results += action(itemMonitor, element)
                    }
                    subMonitor.reportProgress(workPerItem)
                }
                return results
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
        }

        inline fun <T, R> TaskMonitor.forEachWithMonitor(work: Double, name: String, elements: List<T>, action: (TaskMonitor, T) -> R) {
            if (elements.isEmpty()) {
                this.reportProgress(work)
                return
            }
            val subMonitor = this.subMonitor(work)
            subMonitor.reportStarted(name)
            val workPerItem = 1.0 / elements.size
            try {
                for (element in elements) {
                    subMonitor.subTask(workPerItem) { itemMonitor ->
                        action(itemMonitor, element)
                    }
                    subMonitor.reportProgress(workPerItem)
                }
            } finally {
                if (!subMonitor.status.state.isTerminal) {
                    subMonitor.reportDone()
                }
            }
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
        val failureCause: Throwable?
    )

}