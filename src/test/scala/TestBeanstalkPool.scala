import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.actor.ActorSystem

import org.beanstalkc.{Beanstalkc, BeanstalkPool, BeanstalkConnect}

object TestBeanstalkPool {
    def main(args: Array[String]) = {
        val system = ActorSystem()

        val pool = new BeanstalkPool("127.0.0.1", BeanstalkConnect.DEFAULT_PORT, 1, 3)
        pool.maxIdleTime = 40
        val client1 = pool.getClient()
        println(client1.listTubes())
        val client2 = pool.getClient()
        println(client2.listTubes())

        var temp: Beanstalkc = null
        system.scheduler.scheduleOnce(1 seconds) {
            temp = pool.getClient()
            println(Thread.currentThread() + " get connection")
        }
//        system.scheduler.scheduleOnce(6 seconds) {
//            temp.close()
//            println(Thread.currentThread() +  " release connection.")
//        }

        TimeUnit.SECONDS.sleep(3)
        println("waiting....")
        val client3 = pool.getClient()
        println(client3.listTubes())

        println(client1.listTubes())  // exception here. socketException
        client1.close()
        client2.close()
        client3.close()

//        system.terminate()
    }
}
