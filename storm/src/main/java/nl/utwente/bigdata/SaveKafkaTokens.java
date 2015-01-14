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

import nl.utwente.bigdata.bolts.TokenizerBolt;
import nl.utwente.bigdata.bolts.TweetJsonToTextBolt;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;


public class SaveKafkaTokens extends AbstractTopologyRunner {   

	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
		    	
		String boltId = "";
		String prevId;
		
		// kafka spout
		boltId = "kafka";
		SpoutConfig spoutConf = new SpoutConfig(new ZkHosts(properties.getProperty("zkhost", "localhost:2181")),
				  properties.getProperty("topic", "worldcup"), // topic to read from
				  "/brokers", // the root path in Zookeeper for the spout to store the consumer offsets
				  "worldcup");
	
		
		spoutConf.scheme = new TweetFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);
		builder.setSpout(boltId, spout); 
		prevId = boltId;
		
		boltId = "textify"; 
		builder.setBolt(boltId, new TweetJsonToTextBolt()).shuffleGrouping(prevId); 
		prevId = boltId;
		
		boltId = "tokenizer"; 
		builder.setBolt(boltId, new TokenizerBolt()).shuffleGrouping(prevId);
		prevId = boltId;
		
		/* 
		 * Configure a file output bolt. Note that this configuration writes to a file 
		 * on the local filesystem - not on Hdfs as the Bolt Name suggests.
		 * */
		boltId = "file";
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        // rotate files when they reach 1MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1, FileSizeRotationPolicy.Units.MB);
        // where to save files and how should they be called
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/tmp/")
                .withExtension(".txt");
        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");
		HdfsBolt bolt = new HdfsBolt()
        .withFsUrl("file://tmp")         // save on local filesystem (testing purposes) 
        .withFileNameFormat(fileNameFormat)
        .withRecordFormat(format)
        .withRotationPolicy(rotationPolicy)
        .withSyncPolicy(syncPolicy)
        .addRotationAction(new MoveFileAction().toDestination("/tmp/")); // move old files 
		
		builder.setBolt(boltId, bolt).shuffleGrouping(prevId);
		prevId = boltId;
		
		StormTopology topology = builder.createTopology();
		return topology;
	}
	
    
    public static void main(String[] args) {
    	new SaveKafkaTokens().run(args);;
    }
}
