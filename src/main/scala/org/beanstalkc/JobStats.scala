package org.beanstalkc

import org.json.JSONObject

class JobStats(stats: JSONObject) {
    def timeouts = stats.getInt("timeouts")
    def id = stats.getLong("id")
    def pri = stats.getLong("pri")
    def reserves = stats.getInt("reserves")
    def ttr = stats.getInt("ttr")
    def tube = stats.getString("tube")
    def releases = stats.getInt("releases")
    def file = stats.getInt("file")
    def age = stats.getInt("age")
    def state = stats.getString("state")
    def buries = stats.getInt("buries")
    def timeLeft = stats.getInt("time-left")
    def delay = stats.getInt("delay")
    def kicks = stats.getInt("kicks")

    override def toString(): String = {
        stats.toString
    }
}