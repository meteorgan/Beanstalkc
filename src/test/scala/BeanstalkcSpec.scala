import java.util.concurrent.TimeUnit

import org.beanstalkc._
import org.scalatest.FlatSpec


class BeanstalkcSpec extends FlatSpec {
    val client = new Beanstalkc("127.0.0.1")

    "put" should "return a Long value" in {
        client.use(Job.DEFAULT_TUBE)

        val id1 = client.put("hello")
        assert(id1.isInstanceOf[Long])

        val id2 = client.put("world".getBytes)
        assert(id2.isInstanceOf[Long])

        val id3 = client.put("hello world".getBytes, 1000, 10, 120)
        assert(id3.isInstanceOf[Long])

        client.delete(id1)
        client.delete(id2)
        client.delete(id3)
    }

    it should "raise exception if a job too big" in {
        val content = "hello world" * 10000
        intercept[BeanstalkException] {
            client.put(content)
        }
    }

    "delete" should "raise exception if delete a job does not exist" in {
        client.use(Job.DEFAULT_TUBE)

        val id = client.put("delete")
        client.delete(id)
        intercept[BeanstalkNotFoundException] {
            client.delete(id)
        }
    }

    "peek" should "return the special job" in {
        client.use(Job.DEFAULT_TUBE)

        val data = "for peek".getBytes
        val id = client.put(data)
        val job = client.peek(id)
        assert(job.getId() == id)
        assert(job.getData().deep == data.deep)

        client.delete(id)
    }

    "stat-job" should "return the job stats if exist" in {
        client.use(Job.DEFAULT_TUBE)

        val id = client.put("stat job")
        val stats = client.statsJob(id)
        assert(stats.id == id)
        assert(stats.pri == Job.DEFAULT_PRIORITY)
        assert(stats.ttr == Job.DEFAULT_TTR)

        client.delete(id)
    }

    "use" should "put job in the special tube" in {
        client.use("useTube")

        val id = client.put("use tube")
        val job = client.statsJob(id)
        assert(job.tube == "useTube")

        client.use(Job.DEFAULT_TUBE)
    }

    "watch" should "return the size of watching list" in {
        val tube = "watchTube"
        val size = client.watch(tube)
        assert(size == 2)
        client.ignore(tube)
    }

    "list tubes watched" should "return the list of watched tubes" in {
        val tube = "testWatch"
        client.watch(tube)
        val tubes = client.listTubesWatched()
        assert(tubes.contains(Job.DEFAULT_TUBE))
        assert(tubes.contains(tube))
        client.ignore(tube)
    }

    "ignore" should "return the size of watched tubes" in {
        val tube = "ignoreTube"
        client.watch(tube)
        val s = client.ignore(tube)
        val tubes = client.listTubesWatched()
        assert(s == tubes.size)
        assert(!tubes.contains(tube))
    }

    it should "throw exception when ignore the last watch" in {
        intercept[BeanstalkException] {
            client.ignore(Job.DEFAULT_TUBE)
        }
    }

    "stats-tube" should "return tube information" in {
        val tube = "statsTube"
        client.use(tube)
        val id1 = client.put("test1")
        val id2 = client.put("test2")
        val stats = client.statsTube(tube)
        assert(stats.name == tube)

        client.delete(id1)
        client.delete(id2)
        client.use(Job.DEFAULT_TUBE)
    }

    "list-tubes" should "list all tubes ever used" in {
        val tube = "listTubes"
        client.watch(tube)
        val tubes = client.listTubes()
        assert(tubes.contains(Job.DEFAULT_TUBE))
        assert(tubes.contains(tube))
        client.ignore(tube)
    }

    "list-tube-used" should "return now used tube" in {
        client.use(Job.DEFAULT_TUBE)
        val t1 = client.listTubeUsed()
        assert(t1 == Job.DEFAULT_TUBE)

        val tube = "tubeUsed"
        client.use(tube)
        val t2 = client.listTubeUsed()
        assert(t2 == tube)
        client.use(Job.DEFAULT_TUBE)
    }

    "reserve" should "get job from the tube" in {
        val tube = "reserve"
        client.use(tube)
        val id = client.put("reserve")
        client.watch(tube)
        client.ignore(Job.DEFAULT_TUBE)

        val job = client.reserve()
        assert(job.getId() == id)

        client.delete(id)
        client.watch(Job.DEFAULT_TUBE)
        client.ignore(tube)
        client.use(Job.DEFAULT_TUBE)
    }
    it should "throw time-out exception when reserve-with-timeout timeout" in {
        val tube = "reserve-with-timeout"
        client.use(tube)
        client.watch(tube)
        client.ignore(Job.DEFAULT_TUBE)

        intercept[BeanstalkTimeoutException] {
            client.reserve(3)
        }

        client.watch(Job.DEFAULT_TUBE)
        client.ignore(tube)
        client.use(Job.DEFAULT_TUBE)
    }

    "release" should "set the job to ready" in {
        client.use(Job.DEFAULT_TUBE)

        client.put("release")
        val job = client.reserve()
        val id = job.getId()
        val stats = client.statsJob(id)
        assert(stats.state == "reserved")

        client.release(id)
        val stats1 = client.statsJob(id)
        assert(stats1.state == "ready")

        client.delete(id)
    }
    it should "set job to delayed if delay is not zero" in {
        client.use(Job.DEFAULT_TUBE)

        client.put("release with delay")
        val job = client.reserve()
        val id = job.getId()
        client.release(id, 1000, 3)
        val stats = client.statsJob(id)
        assert(stats.state == "delayed")

        client.delete(id)
    }

    "bury" should "put the job into buried state" in {
        client.use(Job.DEFAULT_TUBE)

        client.put("bury")
        val job = client.reserve()
        val id = job.getId()
        client.bury(id, 1000)
        val stats = client.statsJob(id)
        assert(stats.state == "buried")
        assert(stats.pri == 1000)

        client.delete(id)
    }

    "kick" should "move jobs to ready queue" in {
        client.use("kick")

        val id = client.put("kick".getBytes, 1000, 20, 120)
        val number = client.kick(1)
        assert(number == 1)
        val stats = client.statsJob(id)
        assert(stats.state == "ready")
        client.delete(id)

        client.use(Job.DEFAULT_TUBE)
    }

    "kick-job" should "kick the special job" in {
        client.put("kick-job")
        val job = client.reserve()
        val id = job.getId()
        client.bury(id)
        client.kickJob(id)
        val stats = client.statsJob(id)
        assert(stats.state == "ready")

        client.delete(id)
        client.use(Job.DEFAULT_TUBE)
    }

    "stats" should "return beanstalk system stats" in {
        val stats = client.stats()
    }

    "pause tube" should "delay all job to be reserved before the period passed" in {
        val tube = "pauseTube"
        client.use(tube)
        client.pauseTube(tube, 5)
        val id = client.put("pause tube")
        client.watch(tube)
        client.ignore(Job.DEFAULT_TUBE)

        intercept[BeanstalkTimeoutException] {
            client.reserve(4)
        }

        client.delete(id)
        client.watch(Job.DEFAULT_TUBE)
        client.ignore(tube)
        client.use(Job.DEFAULT_TUBE)
    }

    "touch" should "postpones the auto release of a reserved job until ttr" in {
        client.use(Job.DEFAULT_TUBE)

        client.put("touch")
        val job = client.reserve()
        val id = job.getId()
        TimeUnit.SECONDS.sleep(3)
        val stat1 = client.statsJob(id)
        client.touch(id)
        val stat2 = client.statsJob(id)
        assert(stat2.timeLeft > stat1.timeLeft)

        client.delete(id)
    }
}