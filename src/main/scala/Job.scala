package org.beanstalkc

class Job(id: Long, data: Array[Byte]) {
    def getId(): Long = id
    def getData(): Array[Byte] = data
    def size(): Int = data.length
}

object Job {
    val DEFAULT_TTR = 120
    val DEFAULT_DELAY = 0
    val DEFAULT_PRIORITY = 1024
    val DEFAULT_TUBE = "default"
}