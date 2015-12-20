package org.beanstalkc

import java.util.concurrent.TimeUnit

import org.json.JSONObject
import org.yaml.snakeyaml.Yaml

class Beanstalkc(conn: BeanstalkConnect) {
    def this(host: String, port: Int) {
        this(new SocketConnect(host, port))
    }

    def this(host: String) {
        this(new SocketConnect(host, BeanstalkConnect.DEFAULT_PORT))
    }

    def this() {
        this(new SocketConnect(BeanstalkConnect.DEFAULT_HOST, BeanstalkConnect.DEFAULT_PORT))
    }

    @throws(classOf[BeanstalkException])
    def put(data: Array[Byte], priority: Long, delay: Int, ttr: Int): Long = {
        val size = data.size
        val command = s"put $priority $delay $ttr $size"
        val expectErr = List("BURIED", "EXPECTED_CRLF", "JOB_TOO_BIG", "DRAINING")
        val response = conn.sendCommand(command, data, "INSERTED", expectErr)
        response(0).toInt
    }

    @throws(classOf[BeanstalkException])
    def put(data: Array[Byte]): Long = {
        put(data, Job.DEFAULT_PRIORITY, Job.DEFAULT_DELAY, Job.DEFAULT_TTR)
    }

    @throws(classOf[BeanstalkException])
    def put(data: String): Long = {
        put(data.getBytes)
    }

    @throws(classOf[BeanstalkException])
    def reserve(): Job = {
        val command = "reserve"
        reserve(command)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkTimeoutException])
    def reserve(timeout: Int): Job = {
        val command = s"reserve-with-timeout $timeout"
        reserve(command)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkTimeoutException])
    private def reserve(command: String): Job = {
        val expectErr = List("DEADLINE_SOON", "TIMED_OUT")
        val response = conn.sendCommand(command, "RESERVED", expectErr)
        val id = response(0).toLong
        val size = response(1).toInt
        val bytes = conn.getBody(size)

        new Job(id, bytes)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def release(id: Long): Unit = {
        release(id, Job.DEFAULT_PRIORITY, Job.DEFAULT_DELAY)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def release(id: Long, pri: Long, delay: Int): Unit = {
        val command = s"release $id $pri $delay"
        val expectErr = List("BURIED", "NOT_FOUND")
        conn.sendCommand(command, "RELEASED", expectErr)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def bury(id: Long, pri: Long): Unit = {
        val command = s"bury $id $pri"
        val expectErr = List("NOT_FOUND")
        conn.sendCommand(command, "BURIED", expectErr)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def bury(id: Long): Unit = {
        bury(id, Job.DEFAULT_PRIORITY)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def touch(id: Long): Unit = {
        val command = s"touch $id"
        val expectErr = List("NOT_FOUND")
        conn.sendCommand(command, "TOUCHED", expectErr)
    }

    @throws(classOf[BeanstalkException])
    def kick(bound: Int): Int = {
        val command = s"kick $bound"
        val expectErr = List[String]()
        val response = conn.sendCommand(command, "KICKED", expectErr)
        response(0).toInt
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def kickJob(id: Long): Unit = {
        val command = s"kick-job $id"
        val expectErr = List("NOT_FOUND")
        conn.sendCommand(command, "KICKED", expectErr)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def delete(id: Long): Unit = {
        val command = s"delete $id"
        val expectErr = List("NOT_FOUND")
        conn.sendCommand(command, "DELETED", expectErr)
    }

    @throws(classOf[BeanstalkException])
    def use(tube: String): Unit = {
        val command = s"use $tube"
        val expectErr = List[String]()
        conn.sendCommand(command, "USING", expectErr)
    }

    @throws(classOf[BeanstalkException])
    def watch(tube: String): Int = {
        val command = s"watch $tube"
        val expectErr = List[String]()
        val response = conn.sendCommand(command, "WATCHING", expectErr)
        response(0).toInt
    }

    @throws(classOf[BeanstalkException])
    def ignore(tube: String): Int = {
        val command = s"ignore $tube"
        val expectErr = List("NOT_IGNORED")
        val response = conn.sendCommand(command, "WATCHING", expectErr)
        response(0).toInt
    }

    @throws(classOf[BeanstalkException])
    def listTubes(): List[String] = {
        val command = "list-tubes"
        val expectErr = List[String]()
        val response = conn.sendCommand(command, "OK", expectErr)
        val size = response(0).toInt
        val body = conn.getBody(size)

        yamlToList(body)
    }

    @throws(classOf[BeanstalkException])
    def listTubeUsed(): String = {
        val command = "list-tube-used"
        val expectErr = List[String]()
        val response = conn.sendCommand(command, "USING", expectErr)
        val name = response(0)

        new String(name)
    }

    @throws(classOf[BeanstalkException])
    def listTubesWatched(): List[String] = {
        val command = "list-tubes-watched"
        val expectErr = List[String]()
        val response = conn.sendCommand(command, "OK", expectErr)
        val size = response(0).toInt
        val body = conn.getBody(size)

        yamlToList(body)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def pauseTube(tube: String, delay: Int): Unit = {
        val command = s"pause-tube $tube $delay"
        val expectErr = List("NOT_FOUND")
        conn.sendCommand(command, "PAUSED", expectErr)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def peek(id: Long): Job = {
        val command = s"peek $id"
        peek(command)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def peekReady(): Job = {
        val command = "peek-ready"
        peek(command)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def peekDelayed(): Job = {
        val command = "peek-delayed"
        peek(command)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def peekBuried(): Job = {
        val command = "peek-buried"
        peek(command)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    private def peek(command: String): Job = {
        val expectErr = List("NOT_FOUND")
        val response = conn.sendCommand(command, "FOUND", expectErr)
        val id = response(0).toLong
        val size = response(1).toInt
        val body = conn.getBody(size)
        new Job(id, body)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def statsJob(id: Long): JobStats = {
        val command = s"stats-job  $id"
        val expectErr = List("NOT_FOUND")
        val response = conn.sendCommand(command, "OK", expectErr)
        val size = response(0).toInt
        val body = conn.getBody(size)

        val json = yamlToJson(body)
        new JobStats(json)
    }

    @throws(classOf[BeanstalkException])
    @throws(classOf[BeanstalkNotFoundException])
    def statsTube(tube: String): TubeStats = {
        val command = s"stats-tube $tube"
        val expectErr = List("NOT_FOUND")
        val response = conn.sendCommand(command, "OK", expectErr)
        val size = response(0).toInt
        val body = conn.getBody(size)

        val json = yamlToJson(body)
        new TubeStats(json)
    }

    @throws(classOf[BeanstalkException])
    def stats(): BeanstalkStats = {
        val command = "stats"
        val expectErr = List[String]()
        val response = conn.sendCommand(command, "OK", expectErr)
        val size = response(0).toInt
        val body = conn.getBody(size)

        val json = yamlToJson(body)
        new BeanstalkStats(json)
    }

    def quit(): Unit = {
        val command = "quit"
        conn.sendCommand(command)
    }

    def close(): Unit = {
        quit()
        conn.close()
    }

    private def yamlToJson(data: Array[Byte]): JSONObject = {
        val str = new String(data)
        val yaml = new Yaml()
        val map = yaml.load(str).asInstanceOf[java.util.Map[String, String]]

        new JSONObject(map)
    }

    private def yamlToList(data: Array[Byte]): List[String] = {
        val str = new String(data)
        val yaml = new Yaml()
        val lst = yaml.load(str).asInstanceOf[java.util.List[String]]

        import collection.JavaConversions._
        lst.toList
    }
}

object Beanstalkc {
    def main(args: Array[String]): Unit = {
        val client = new Beanstalkc("127.0.0.1")
        val tube = "test_pause_tube"
        client.use(tube)
        client.watch(tube)
        client.ignore(Job.DEFAULT_TUBE)
        println("before pause")
        client.put("before pause")
        client.pauseTube(tube, 5)
        println("after pause")
        client.put("after pause1")
        client.put("after pause2")

        println("reserve1")
        val job1 = client.reserve()
        println("reserve2")
        val job2 = client.reserve()

        println("reserve3")
        val job3 = client.reserve(2)
        println("after reserve3")
        println(client.statsJob(job3.getId()))
        client.delete(job3.getId())

        println(client.statsTube(tube))
    }
}