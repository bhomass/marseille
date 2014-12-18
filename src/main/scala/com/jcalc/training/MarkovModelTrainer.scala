package com.jcalc.training

import java.util.Properties
import java.util.Scanner
import scala.Array._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

class MarkovModelTrainer(sparkContext : SparkContext) {

  val is = getClass.getResourceAsStream("/markov_states.txt")
  val stateScanner = new Scanner(is);
  val line = stateScanner.nextLine()
  val states = line.split(",");

  val probMatrix = ofDim[Double](states.length, states.length)

  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("/usr/local/hadoop-2.5.1/etc/hadoop/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/usr/local/hadoop-2.5.1/etc/hadoop/hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  // read sequence in
  val (modelPath, sequenceFile, hdfsAddr) =
    try {
      val prop = new Properties()
      val propertyIs = getClass.getResourceAsStream("/app_config.properties")
      prop.load(propertyIs)

      (
        prop.getProperty("model.path"),
        prop.getProperty("sequence.file"),
        prop.getProperty("hdfs.addr"))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }

  def generateModel() {

    val statesVar = sparkContext.broadcast(states)

    val sequences = sparkContext.textFile("hdfs://" + hdfsAddr + "/" + sequenceFile)
      .map {
        line =>
          val parts = line.split(",")

          val userId = parts(0)
          val records = parts.drop(1)

          (userId, records)
      }

    val transitionMatrix = sequences.map {
      line =>
        val userId = line._1
        val records = line._2
        // each line goes into a loop
        // every pair transforms into key, value pair
        // transition key = tuple (sourceIndex, destinationIndex)
        // value = 1

        var stateCount = ArrayBuffer[String]()
        val numberRecords = records.length

        for (i <- 0 to numberRecords - 2) {
          val statesV = statesVar.value;
          val srcIndex = statesV.indexOf(records(i))

          val destIndex = statesV.indexOf(records(i + 1))
          val key = srcIndex + "-" + destIndex

          stateCount += key
        }
        stateCount.toList
    }

      .flatMap(c => c)
      .map(c => (c, 1))
      // reduce by key, add values
      .reduceByKey(_ + _)

    val rowNormalizationVector = collection.mutable.Map[String, Int]()
    // transform into new key which is first part of key tuple (row)
    transitionMatrix.map {
      x =>
        val src = x._1.split("-")(0)
        (src, x._2)
    }
      // reducybykey, add up values -> normalization values keyed by row
      .reduceByKey(_ + _)
      .collect.foreach(x =>
        rowNormalizationVector += (x._1 -> x._2))

    val rowNormalizationVectorVar = sparkContext.broadcast(rowNormalizationVector)

    transitionMatrix.map {
      x =>
        val normalizationV = rowNormalizationVectorVar.value
        val srcDest = x._1.split("-")
        val srcIndex = srcDest(0).toInt
        val destIndex = srcDest(1).toInt
        val prob = x._2.toDouble / normalizationV(srcDest(0))
        (srcIndex, destIndex, prob)
    }

      .collect.foreach(x => {
        probMatrix(x._1)(x._2) = x._3
      })

    val buf = new StringBuilder
    // final output to hdfs location
    for (i <- 0 to states.length - 1) {
      for (j <- 0 to states.length - 1) {
        buf.append(probMatrix(i)(j) + ",")
      }
      buf.append("\n")
    }

    val out = fileSystem.create(new Path(modelPath))
    out.writeBytes(buf.toString)

  }

}

object MarkovModelTrainer extends App {
    val sparkConf = new SparkConf()
    	//.setMaster("local[4]")		// uncomment this to run in the IDE
    	.setAppName("MarkovModelTrainer")
    	.set("spark.executor.memory", "1g")
  val sparkContext = new SparkContext(sparkConf)
    
  val markovModelTrainer = new MarkovModelTrainer(sparkContext)

  markovModelTrainer.generateModel()
}
