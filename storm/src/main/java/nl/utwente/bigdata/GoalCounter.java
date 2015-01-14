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

import nl.utwente.bigdata.bolts.FilterBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
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

public class GoalCounter extends AbstractTopologyRunner {

	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();

		// Read tweets from kafka
		SpoutConfig spoutConf = new SpoutConfig(new ZkHosts(
				properties.getProperty("zkhost", "localhost:2181")),
				properties.getProperty("topic", "worldcup"), // topic to read
																// from
				"/brokers", // the root path in Zookeeper for the spout to store
							// the consumer offsets
				"worldcup");
		
		spoutConf.forceFromStart = true;
		spoutConf.scheme = new TweetFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);
		builder.setSpout("kafka", spout);
		
		// Extract tweet text
		builder.setBolt("text", new TweetJsonToTextBolt()).shuffleGrouping("kafka");
		
		// Filter on content
		//builder.setBolt("goals", new FilterBolt("goal")).shuffleGrouping("text");
		
		// Output
		/*SyncPolicy syncPolicy = new CountSyncPolicy(1000);
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1,
				FileSizeRotationPolicy.Units.KB); // rotate files when they
													// reach 1KB
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(
				"/output/").withExtension(".txt");
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter("|"); // use "|" instead of "," for field
											// delimiter

		HdfsBolt bolt = new HdfsBolt()
				.withFsUrl("hdfs://localhost:8020")
				.withFileNameFormat(fileNameFormat)
				.withRecordFormat(format)
				.withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy)
				.addRotationAction(
						new MoveFileAction().toDestination("/output/old/"));

		builder.setBolt("output", bolt, 1).shuffleGrouping("text");*/
		
		builder.setBolt("print", new PrinterBolt()).shuffleGrouping("text");
		
		return builder.createTopology();
	}

	public static void main(String[] args) {

		String[] a = new String[2];
		a[0] = "GoalCounter";
		a[1] = "local";

		new GoalCounter().run(a);
	}
}
