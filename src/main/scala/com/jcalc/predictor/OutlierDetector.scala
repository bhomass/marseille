package com.jcalc.predictor

import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.util.Scanner
import org.apache.log4j.Logger
import java.util.HashMap
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.producer.Producer

class OutlierDetector(stateSeqWindowSize : Int) {

  val (kafkaFraudTopic, metadataBrokerList, metricThreshold) =
    try {
      val appProp = new Properties()
      val propertyIs = getClass.getResourceAsStream("/app_config.properties")
      appProp.load(propertyIs)

      (
        appProp.getProperty("kafka.fraudtopic"),
        appProp.getProperty("metadata.broker.list"),
        appProp.getProperty("metric.threshold").toDouble
        )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }

  val props = new Properties();

  props.put("metadata.broker.list", metadataBrokerList);
  props.put("serializer.class", "kafka.serializer.StringEncoder");
  val config = new ProducerConfig(props);
  val producer = new Producer[String, String](config)
  
  private val logger = Logger.getLogger(getClass().getName());

  private val records = new HashMap[String, ArrayBuffer[String]]();
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("/usr/local/hadoop-2.5.1/etc/hadoop/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/usr/local/hadoop-2.5.1/etc/hadoop/hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  val (modelPath, algorithm) =
    try {
      val prop = new Properties()
      val propertyIs = getClass.getResourceAsStream("/app_config.properties")
      prop.load(propertyIs)

      (
        prop.getProperty("model.path"),
        prop.getProperty("scorer.algorithm")
      )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }
    
  val is = getClass.getResourceAsStream("/markov_states.txt")
  val stateScanner = new Scanner(is);
  val line = stateScanner.nextLine()
  val states = line.split(",");
  val numStates = states.length;
  val stateTranstionProb = Array.ofDim[Double](numStates, numStates)
  logger.info("numStates:" + numStates);

  private val fileSystem = FileSystem.get(conf)

  // pick up markov model
  val path = new Path(modelPath)
  val modelIs = fileSystem.open(path)

  val scanner = new Scanner(modelIs)

  var row = 0;

  while (scanner.hasNextLine()) {
    val line = scanner.nextLine();
      deseralizeTableRow(stateTranstionProb, line, ",", row, numStates);
      row += 1
  }

  scanner.close();

  val transitions = stateSeqWindowSize - 1

  val scorer = Scorer(algorithm, stateTranstionProb, states, stateSeqWindowSize)

  def deseralizeTableRow(table: Array[Array[Double]], data: String, delim: String, row: Int, numCol: Int) {
    val items = data.split(delim);
    if (items.length != numCol) {
      throw new IllegalArgumentException(
        "Row serialization failed, number of tokens in string does not match with number of columns");
    }
    for (column <- 0 to numCol - 1) {
      stateTranstionProb(row)(column) = items(column).toDouble;
    }
  }

  def execute(entityID: String, records: String) {
//    println("inside detector.execute: entityId = " + entityID +", records = " + records)
    var score: Double = 0.0;

    val recordSeq = records.split(",")

    if (recordSeq.size >= stateSeqWindowSize) {
      
      for (j <- 0 to recordSeq.size - stateSeqWindowSize) {

      score = scorer.calcScore(recordSeq, j) / transitions
	    //outlier
	    if (score > metricThreshold) {
	      val stBld = new StringBuilder(entityID);
	      stBld.append(" : ");
	      for (k <- 0 to stateSeqWindowSize - 1) {
	        stBld.append(recordSeq(k + j)).append(" ");
	      }
	      stBld.append(": ");
	      stBld.append(score);
	      //			jedis.lpush(outputQueue,  stBld.toString());
	      // write to hbase and push to kafka
	      println(stBld.toString)
	      sendToKafkaOutput(stBld.toString)
	    }
      }
    }

  }

  private def sendToKafkaOutput(messageBody : String) {
        val data = new KeyedMessage[String, String](kafkaFraudTopic, messageBody)
        producer.send(data);

  }
  

}

object OutlierDetector {

  def main(args: Array[String]) {
    val stateSeqWindowSize = 5
    val detector = new OutlierDetector(stateSeqWindowSize)
    val is = classOf[OutlierDetector].getResourceAsStream("/test_sequence.txt")
    val sequenceScanner = new Scanner(is);
    while (sequenceScanner.hasNext()) {
      val line = sequenceScanner.nextLine()
      val firstComma = line.indexOf(",")
      val userId = line.substring(0, firstComma)
      val records = line.substring(firstComma + 1)
      detector.execute(userId, records)
    }
  }

}