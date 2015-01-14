/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.utwente.bigdata;

import java.util.Properties;

import nl.utwente.bigdata.bolts.CheckGoalBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class ScoreSummarizer extends AbstractTopologyRunner {   
	
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
      
		String boltId = "";
		String prevId;
		
		//Set up the kafka spout
		boltId = "kafka";
		SpoutConfig spoutConf = new SpoutConfig(
			new ZkHosts(properties.getProperty("zkhost", "localhost:2181")),
			properties.getProperty("topic", "worldcup"), 
			"/brokers", 
			"worldcup"
		);

		spoutConf.scheme = new TweetFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);
		
		//Add the kafka spout
		builder.setSpout(boltId, spout); 
		prevId = boltId;
		
		
		//Check the location and pass if it states goal for a given country
		boltId = "checkGoal";
		builder.setBolt(boltId, new CheckGoalBolt()).shuffleGrouping(prevId); // "tweet" -> "country"
		prevId = boltId;
		
		
//		//Count the
//		boltId = "counter";
//		builder.setBolt(boltId, new TokenizerBolt()).shuffleGrouping(prevId); // "words" -> "word"
//		prevId = boltId;
//		
//		
//		boltId = "topcounter";
//		builder.setBolt(boltId, new TopCounterBolt(25)).fieldsGrouping(prevId, new Fields("word")); // "word" -> "word", "count"
//		prevId = boltId;
		
		/*
		 * OUTPUT 1: hdfs
		 */
//		boltId = "file";
//        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
//        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1, FileSizeRotationPolicy.Units.KB);  // rotate files when they reach 1KB        
//        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/martijn/tweets/").withExtension(".txt");
//        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|"); // use "|" instead of "," for field delimiter
//
//		HdfsBolt bolt = new HdfsBolt()
//        .withFsUrl("hdfs://localhost:8020")
//        .withFileNameFormat(fileNameFormat)
//        .withRecordFormat(format)
//        .withRotationPolicy(rotationPolicy)
//        .withSyncPolicy(syncPolicy)
//        .addRotationAction(new MoveFileAction().toDestination("/user/martijn/old/"));
//		
//		builder.setBolt(boltId, bolt).shuffleGrouping(prevId);

		/*
		 * OUTPUT 2: standard out
		 */
//		boltId = "print";
//		builder.setBolt(boltId, new PrinterBolt()).shuffleGrouping(prevId);

		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	
    	String[] a = new String[2];
    	a[0] = "ScoreSummarizer";
    	a[1] = "local";
    	
    	new ScoreSummarizer().run(a);
    }
}
