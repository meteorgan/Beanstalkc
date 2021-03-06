name := "Beanstalkc"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
    "org.yaml" % "snakeyaml" % "1.15",
    "org.json" % "json" % "20140107",
    "com.typesafe.akka" % "akka-actor_2.11" % "2.4.1",
    "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
)