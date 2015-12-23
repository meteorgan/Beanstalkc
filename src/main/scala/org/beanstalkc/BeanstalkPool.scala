package org.beanstalkc

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorSystem

class BeanstalkPool(val host: String, val port: Int, val minPoolSize: Int, val maxPoolSize: Int) {
    def this(host: String, port: Int) = this(host, port, 3, 10)

    private val lock = new Object
    private val usedClients = collection.mutable.Set[Beanstalkc]()
    private val notUsedClients = collection.mutable.Set[Beanstalkc]()
    for(i <- 0 until minPoolSize)
        notUsedClients += new Beanstalkc(host, port)

    private val system = ActorSystem("BeanstalkPool_")
    system.scheduler.schedule(30 seconds, 30 seconds) {
        lock.synchronized {
            val current = System.currentTimeMillis()
            val toRemoved = usedClients.filter(current - _.lastUsed > maxIdleTime)
            usedClients --= toRemoved
            toRemoved.foreach(_.conn.close())
            if(toRemoved.nonEmpty)
                lock.notifyAll()
        }
    }


    private var _maxIdleTime = 10 *60 * 1000

    def maxIdleTime = _maxIdleTime

    def maxIdleTime_=(value: Int): Unit = {
        _maxIdleTime = value
    }


    def getClient(): Beanstalkc = {
        lock.synchronized {
            while(usedClients.size == maxPoolSize) {
                lock.wait()
            }

            while(notUsedClients.size + usedClients.size < minPoolSize) {
                notUsedClients += new Beanstalkc(host, port)
            }
            var client: Beanstalkc = null
            if (notUsedClients.isEmpty) {
                client = new Beanstalkc(host, port)
                usedClients += client
            }
            else {
                client = notUsedClients.head
                notUsedClients -= client
                usedClients += client
            }
            client.pool = this
            client.lastUsed = System.currentTimeMillis()
            client
        }
    }

    // 回收失效的连接
    def destroyClient(client: Beanstalkc): Unit = {
        lock.synchronized {
            client.conn.close()
            usedClients -= client
            lock.notifyAll()
        }
    }

    // 回收正常连接
    def reapClient(client: Beanstalkc): Unit = {
        lock.synchronized {
            if(usedClients.contains(client)) {
                usedClients -= client
                notUsedClients += client
                lock.notifyAll()
            }
        }
    }
}

object BeanstalkPool extends scala.App {
    val pool = new BeanstalkPool("127.0.0.1", BeanstalkConnect.DEFAULT_PORT, 1, 3)
    val client1 = pool.getClient()
    println(client1.listTubes())
    val client2 = pool.getClient()
    println(client2.listTubes())
    val client3 = pool.getClient()
    println(client3.listTubes())

    client1.close()
    client2.close()
    client3.close()
}