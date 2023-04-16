package org.chronos.chronostore.async.tasks

interface AsyncTask {

    val name: String

    fun run(monitor: TaskMonitor)

}