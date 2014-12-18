package com.jcalc.feed;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CCStreamingFeed {
	private static final int TOKENLENGTH = 10;
	private static final int MAXTRANSACTIONS = 15;
	private static final String[] AmountTokens = {"L", "N", "H"};
	private static final int[] AmountProbs = {35, 53, 12};
	private static final String[] ItemPriceTokens = {"N", "H"};
	private static final int[] ItemPriceProbs = {85, 15};
	private static final String[] TimeLapseTokens = {"L", "N", "S"};
	private static final int[] TimeLapseProbs = {35, 45, 20};

	private int numNewCustomers;
	private TokenGenerator amountGenerator;
	private TokenGenerator itemPriceGenerator;
	private TokenGenerator timeLapseGenerator;
	private Producer<String, String> producer;
	
	private String kafkaInputTopic;
	
	public static void main(String[] args) {
		int numCustomers = Integer.parseInt(args[0]);
		
		List<String> existingCustomers = new ArrayList<String>();
		
		Configuration hbaseconf = HBaseConfiguration.create();
		try {
		  HTable table = new HTable(hbaseconf, "cchistory");
	      Scan scan = new Scan();
	      
	      scan.addColumn(Bytes.toBytes("transactions"), Bytes.toBytes("records"));
	      
	      scan.setFilter(new PageFilter(numCustomers));

	      scan.setMaxVersions(1);
	      ResultScanner scanner = table.getScanner(scan);
	      
	      for (Result result : scanner) {
	    	  String customerId = Bytes.toString(result.getRow());
	    	  existingCustomers.add(customerId);
	    	  
	      }
	      table.close();
		} catch (IOException ex){
			ex.printStackTrace();
		}
	    
		int numNewCustomers = numCustomers - existingCustomers.size();
		CCStreamingFeed feeder = new CCStreamingFeed(numNewCustomers);
		List<String>  dataFeed = feeder.generateInputFeed(existingCustomers);
		for (String inputLine : dataFeed){
			feeder.feedInputQueue(inputLine);
		}
				
	}

	public CCStreamingFeed(int numNewCustomers){
		this.numNewCustomers = numNewCustomers;
		
		InputStream is = getClass().getClassLoader().getResourceAsStream("app_config.properties");
		Properties appProp = new Properties();
		String metadataBrokerList = "localhost:9092";
		try {
			appProp.load(is);
			metadataBrokerList = appProp.getProperty("metadata.broker.list");
			kafkaInputTopic = appProp.getProperty("kafka.inputtopic");
		} catch (IOException ex){
			ex.printStackTrace();
		}
		
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", metadataBrokerList);
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(producerProps); 
		this.producer = new Producer<String, String>(config);
		
	}
	
	public List<String> generateInputFeed(){
		return generateInputFeed(new ArrayList<String>());
	}
	
	private List<String> generateInputFeed(List<String> existingCustomers){
		// first randomly select existing customers in hbase
		int totalCustomers = numNewCustomers + existingCustomers.size();
		List<String> dataFeed = new ArrayList<String>();
		
		String[] customerIDs = new String[totalCustomers];
		for (int i = 0; i < numNewCustomers; i++){
			customerIDs[i] = generateRandomId();
		}
		int j = 0;
		for (int i = numNewCustomers; i < totalCustomers; i++){
			customerIDs[i] = existingCustomers.get(j++);
		}
		
		amountGenerator = new TokenGenerator(AmountTokens, AmountProbs);
		itemPriceGenerator = new TokenGenerator(ItemPriceTokens, ItemPriceProbs);
		timeLapseGenerator = new TokenGenerator(TimeLapseTokens, TimeLapseProbs);
		
		for (int t = 0; t < MAXTRANSACTIONS; t++){
			for (int c = 0; c < totalCustomers; c++){
				if (getsTransaction()){
					String customerId = customerIDs[randomPick(totalCustomers)];
					String transId = generateRandomId();
					String inputLine = customerId + "," + transId + "," + amountGenerator.nextToken() + itemPriceGenerator.nextToken() + timeLapseGenerator.nextToken();

					dataFeed.add(inputLine);
				}
			}
		}

		return dataFeed;
	}
	
	private boolean getsTransaction(){
		return Math.random() < 0.5;
	}
	
	private int randomPick(int choices){
		int selection = (int)(Math.random() * choices);
		return selection;
	}
	
	private String generateRandomId(){
		return RandomStringUtils.random(TOKENLENGTH, "ABCDEFGHIJKLMNOPQRSTUVWXY0123456789");
	}

	
	private void feedInputQueue(String inputLine){
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(kafkaInputTopic, inputLine);
		producer.send(data);
	}

}
