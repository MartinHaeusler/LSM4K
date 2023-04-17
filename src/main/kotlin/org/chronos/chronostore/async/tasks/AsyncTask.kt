package org.chronos.chronostore.async.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor

interface AsyncTask {

    val name: String

    fun run(monitor: TaskMonitor)

}