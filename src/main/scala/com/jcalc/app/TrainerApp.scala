package com.jcalc.app

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import com.jcalc.training.MarkovModelTrainer
import com.jcalc.feed.CCStreamingFeed
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkConf

object TrainerApp extends App {

  val (numTrainingCustomers, trainingDataFile, sequenceFile, hdfsAddr) =
    try {
      val prop = new Properties()
      val propertyIs = getClass.getResourceAsStream("/app_config.properties")
      prop.load(propertyIs)

      (
        prop.getProperty("num.training.customers").toInt,
        prop.getProperty("transaction.file"),
        prop.getProperty("sequence.file"),
        prop.getProperty("hdfs.addr"))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }

  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("/usr/local/hadoop-2.5.1/etc/hadoop/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/usr/local/hadoop-2.5.1/etc/hadoop/hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)
  conf.set("fs.hdfs.impl",
    "org.apache.hadoop.hdfs.DistributedFileSystem")

  conf.set("fs.file.impl",
    "org.apache.hadoop.fs.LocalFileSystem")

  private val fileSystem = FileSystem.get(conf)

  val feeder = new CCStreamingFeed(numTrainingCustomers)
  val dataFeed = feeder.generateInputFeed();
  // write training data out to hdfs

    val sparkConf = new SparkConf()
    	.setMaster("local[4]")		// uncomment this to run in the IDE
    	.setAppName("Trainer App")
    	.set("spark.executor.memory", "1g")
  val sparkContext = new SparkContext(sparkConf)
  val dataRdd = sparkContext.parallelize(dataFeed, 2)

  dataRdd.saveAsTextFile("hdfs://" + hdfsAddr + "/" + trainingDataFile)

  val xactionByCustomer = dataRdd.map {
    line =>
      {
        val parts = line.split(",")
        (parts(0), parts(2)) // drop transaction id
      }
  }.reduceByKey((previous, current) => previous + "," + current)
  .map {
    tuple =>
      val line = tuple._1 + "," + tuple._2
      line
  }

  // write sequence data out to hdfs
  xactionByCustomer.saveAsTextFile("hdfs://" + hdfsAddr + "/" + sequenceFile)

  val markovModelTrainer = new MarkovModelTrainer(sparkContext)

  markovModelTrainer.generateModel()

}