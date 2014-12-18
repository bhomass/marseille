import AssemblyKeys._

name := "spark-kafka-fraud-detection"

lazy val commonSettings = Seq(
  version := "1.0",
  organization := "com.jcalc",
  scalaVersion := "2.10.4"
)

val sparkVersion = "1.1.0"

lazy val app = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    // your settings here
  )

libraryDependencies <<= scalaVersion {
  scala_version => Seq(
    // Spark and Spark Streaming
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
    // Kafka
    "org.apache.kafka" %% "kafka" % "0.8.1.1",
     // Apache Commons Lang Utils
    "org.apache.commons" % "commons-lang3" % "3.3.2",  
    "org.apache.hadoop" % "hadoop-client" % "2.4.0",
    "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0",
    "org.apache.hbase" % "hbase-client" % "0.98.7-hadoop2",
    "org.apache.hbase" % "hbase-common" % "0.98.7-hadoop2"
  )
}

resolvers += "typesafe repo" at " http://repo.typesafe.com/typesafe/releases/"

packSettings

packMain := Map(
  "feed" -> "com.jcalc.feed.CCStreamingFeed"
)

