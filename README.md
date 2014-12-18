An example implementation of a Markov chain based credit card fraud detection

More information on our blog: http://

In order to run our example, we need to install the followings:

* [Scala 2.10+](http://www.scala-lang.org/)
* [SBT](http://www.scala-sbt.org/)
* [Apache Zookeeper](http://zookeeper.apache.org/)
* [Apache Kafka](http://kafka.apache.org/)
* [Apache Spark 1.1.0](https://spark.apache.org/docs/1.1.0/spark-standalone.html)

Building the examples:
    
    $ sbt assembly - this creates [installation_dir]/marseille/target/scala-2.10/spark-kafka-fraud-detection-assembly-1.0.jar
    
Let's start all the components running.
    
Start spark in standalone mode

    $ [spark_installation_dir]/sbin/start-all.sh
    
you should be able to bring up the spark monitoring page at http://localhost:8080 at this point. The page should indicate that there is one work node (default setting).
    
Start hbase

    $ [hbase_installation_dir]/bin/start-hbase.sh
    
This would also start the attached zookeeper instance.
    
Start kafka. this will reuse the zookeeper instance started with hbase. Do not start a second instance.

    $ [kafka_installation_dir]/bin/kafka-server-start.sh config/server.properties
    
Start hdfs

    $ [hadoop_installation_dir]/sbin/start-all.sh
    
Use jps to check both spark and hbase are running

    $ jps
 
You should see lines like
	10461 Kafka
	10322 HRegionServer
	14175 Jps
	13669 Main
	97904 
	10222 HMaster
	13893 Master
	13546 SecondaryNameNode
	13452 DataNode
	13377 NameNode
	
Now start all the monitoring tools to keep an eye on each stage of execution
	    
Create 3 kafka topics
    $ [kafka_installation_dir]/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic real-time-cc
    $ [kafka_installation_dir]/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-trans-history
    $ [kafka_installation_dir]/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic candidate-fraud-trans
    
Start 3 listeners for these 3 topics using kafka's console consumer
    $ [kafka_installation_dir]/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic real-time-cc
    $ [kafka_installation_dir]/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic user-trans-history
    $ [kafka_installation_dir]/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic candidate-fraud-trans
    
The first task to run is creating a training data set.
    
Run the TrainerApp to create the training set. The number of customers is set to 500 by default in the app_config.prop file
    $ /opt/spark-1.1.0-bin-hadoop2.4/bin/spark-submit --class com.jcalc.app.TrainerApp --master spark://yourhostname:7077 spark-kafka-fraud-detection-assembly-1.0.jar 
    
this creates three hdfs files : 
    	/data/streaming_analysis/markovmodel.txt  - this is the model file. it contains the transition probability for all state pairs
    	/data/streaming_analysis/training_sequence - this is the raw transaction sequence
    	/data/streaming_analysis/training_transaction  - this is transaction sequence, which groups all transaction sequence per customer
    
Use spark-submit to start MarkovPredictor which listens on the real-time-cc topic, and publish to topics user-trans-history and candidate-fraud-trans
    $ /opt/spark-1.1.0-bin-hadoop2.4/bin/spark-submit --class com.jcalc.feed.MarkovPredictor --master spark://yourhostname:7077 spark-kafka-fraud-detection-assembly-1.0.jar
    
Finally, feed simulated credit card transaction data into the real-time-cc topic to feed MarkovPredictor. 50 is a typical number of customers to use.
    $java -cp spark-kafka-fraud-detection-assembly-1.0.jar com.jcalc.feed.CCStreamingFeed 50
    
When you feed this simulated data, you should see data being printed out on all three topics. In addition, you can check that the transaction history is recorded in hbase 'cchistory' table. Use hbase shell to see the records.
    $ [hbase_install_dir]/bin/hbase shell
    hbase(main)> scan 'cchistory'
    
Hbase record dump looks like the following
    
     UHRMHKBO1D                               column=transactions:records, timestamp=1418713010152, value=LNS,NNL,NNS,LHL,NNN,LHL,NNL,LNN,NNN,NNS,LNN                 
	 UM2RPYPHRX                               column=transactions:records, timestamp=1418713010327, value=NNL,LHN,NNN,NNL,NNN,NNL,NHL,NNN,NNS,NNL,NNN,LHN,LNL         
	 W19EW2550B                               column=transactions:records, timestamp=1418713010198, value=NNN,HHN,NNN,NHL,NNS,NNL,NHN,NNL,HNL,LNS,LNL                 
	 X30RNTW3KG                               column=transactions:records, timestamp=1418713010215, value=NNN,LNN,NNL,LHS,LNN,NNL,NNN,LHN,LNN                         
	 XM7YLRIDL8                               column=transactions:records, timestamp=1418713010173, value=LNN,LNL,NNL,NNL,LNL,NNL,HNN,NNN,NNN,NNN,LNL                 
	 Y3LNSKDK02                               column=transactions:records, timestamp=1418713010207, value=NNS,NNN,NNN,NNN,HNN,LNN,HNL,NNL,NNS,NNL,NNL,LNN,LNN,LHL,NNN 
	50 row(s) in 0.0520 seconds
    
Repeat the simulated data feed as often as you like.

