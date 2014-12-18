package com.jcalc.feed

import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.consumer.ConsumerConfig
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.serializer.StringDecoder
import kafka.producer.KeyedMessage
import com.jcalc.predictor.OutlierDetector
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf

object MarkovPredictor {

    val (kafkaTransTopic, kafkaHost, kafkaPort, kafkaTimeout, kafkaReceiveBuffer, kafkaInputTopic, batchDuration, metadataBrokerList,
        zookeeperConnect, groupId, zookeeperConnectionTimeoutMs, zookeeperSessionTimeoutMs, zookeeperSyncTimeMs, autoCommitIntervalMs, stateSeqWindowSize) =
    try {
      val appProp = new Properties()
      val propertyIs = getClass.getResourceAsStream("/app_config.properties")
      appProp.load(propertyIs)

      (
        appProp.getProperty("kafka.xacttopic"),
        appProp.getProperty("kafka.host"),
        appProp.getProperty("kafka.port"),
        appProp.getProperty("kafka.timeout"),
        appProp.getProperty("kafka.receivebuffer"),
        appProp.getProperty("kafka.inputtopic"),
        Seconds(appProp.getProperty("batch.duration").toLong),
        appProp.getProperty("metadata.broker.list"),
        appProp.getProperty("zookeeper.connect"),
        appProp.getProperty("group.id"),
        appProp.getProperty("zookeeper.connection.timeout.ms"),
        appProp.getProperty("zookeeper.session.timeout.ms"),
        appProp.getProperty("zookeeper.sync.time.ms"),
        appProp.getProperty("auto.commit.interval.ms"),
        appProp.getProperty("state.seq.windowSize").toInt

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

    // send to predictor
  val detector = new OutlierDetector(stateSeqWindowSize)

  
  private def callDetector(entityId : String, records : String) {
      detector.execute(entityId, records)
  }

  private def sendToKafkaOutput(xhistory: RDD[(String, String)]) {
    xhistory.foreach {
      h =>
        val data = new KeyedMessage[String, String](kafkaTransTopic, h._1 + "." + h._2)
        producer.send(data);
    }
  }
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
//    	.setMaster("local[4]")		// uncomment this to run in the IDE
    	.setAppName("CustomerXAggregator")
    	.set("spark.executor.memory", "1g")
    	.set("spark.eventLog.enabled", "true")
    	.set("spark.eventLog.dir ", "sparklogs")
  val sparkContext = new SparkContext(sparkConf)

  // we discretize the stream in BatchDuration seconds intervals
  val streamingContext = new StreamingContext(sparkContext, batchDuration)

  val kafkaParams = Map(
    "zookeeper.connect" -> zookeeperConnect,
    "zookeeper.connection.timeout.ms" -> zookeeperConnectionTimeoutMs,
    "zookeeper.session.timeout.ms" -> zookeeperSessionTimeoutMs,
    "zookeeper.sync.time.ms" -> zookeeperSyncTimeMs,
    "auto.commit.interval.ms" -> autoCommitIntervalMs,
    "group.id" -> groupId)

  val topics = Map(
    kafkaInputTopic -> 1)
  // stream of (topic, SingleTransaction)
  val messages = KafkaUtils.createStream[String, SingleTransaction, StringDecoder, SingleTransactionDecoder](streamingContext, kafkaParams, topics, StorageLevel.MEMORY_ONLY)

  // create key value RDD out of CustomerId and Tokens
  val xactionByCustomer = messages.map(_._2).map {
    transaction =>
      val key = transaction.customerId
      var tokens = transaction.tokens
      (key, tokens)
  }

  // aggregate all tokens per customer
  import org.apache.spark.streaming.StreamingContext._
  val customerHistory = xactionByCustomer.reduceByKey((previous, current) => previous + "," + current)
  
  customerHistory.foreachRDD(sendToKafkaOutput(_))

  customerHistory.foreachRDD {
    tuple =>

      tuple.foreachPartition {
        iterator =>

          // read prev windowSize -1 transactions from hbase and prepend to new token list
          val conf = HBaseConfiguration.create();
          val table = new HTable(conf, "cchistory");
          iterator.foreach {
            tuple =>
              val key = tuple._1
              var tokens = tuple._2
              val get = new Get(Bytes.toBytes(key));
              get.addFamily(Bytes.toBytes("transactions"));
              val result = table.get(get);
              val keyValues = result.getColumnLatest(Bytes.toBytes("transactions"), Bytes.toBytes("records"))
              var archivedRecords: String = ""
              if (keyValues != null) {
                archivedRecords = Bytes.toString(keyValues.getValue())
              }
              var archivedRecordArray = archivedRecords.split(",")
              if (archivedRecords.equals("")) {
                 // do nothing
              } else if (archivedRecordArray.length < stateSeqWindowSize) {
                tokens = archivedRecords.concat(",").concat(tokens)
              } else {
                val startIndex = archivedRecordArray.length - stateSeqWindowSize + 1
                archivedRecordArray = archivedRecordArray.slice(startIndex, archivedRecordArray.length)
                val truncatedArchivedReocrds = archivedRecordArray.mkString(",")
                tokens = truncatedArchivedReocrds.concat(",").concat(tokens)

              }
              val put = new Put(Bytes.toBytes(key));
              put.add(Bytes.toBytes("transactions"), Bytes.toBytes("records"), Bytes.toBytes(tokens));
              table.put(put);
              
              callDetector(key, tokens)
          }
          table.flushCommits()
          table.close()

      }
  }
      
  streamingContext.start()
  }

}