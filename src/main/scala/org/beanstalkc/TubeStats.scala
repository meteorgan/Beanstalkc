package org.beanstalkc

import org.json.JSONObject

class TubeStats(stats: JSONObject) {
    def name = stats.getString("name")
    def currentJobsUrgent = stats.getInt("current-jobs-urgent")
    def currentJobsReady = stats.getInt("current-jobs-ready")
    def currentJobsReserved = stats.getInt("current-jobs-reserved")
    def currentJobsDelayed = stats.getInt("current-jobs-delayed")
    def currentJobsBuried = stats.getInt("current-jobs-buried")
    def totalJobs = stats.getInt("total-jobs")
    def currentUsing = stats.getInt("current-using")
    def currentWaiting = stats.getInt("current-waiting")
    def currentWatching = stats.getInt("current-watching")
    def pause = stats.getInt("pause")
    def cmdDelete = stats.getInt("cmd-delete")
    def cmdPauseTube = stats.getInt("cmd-pause-tube")
    def pauseTimeLeft = stats.getInt("pause-time-left")

    override def toString(): String = {
        stats.toString()
    }
}
