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

import nl.utwente.bigdata.bolts.NGramerBolt;
import nl.utwente.bigdata.bolts.TweetJsonToTextBolt;
import nl.utwente.bigdata.spouts.JsonSpout;

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

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class Assignment4_1 extends AbstractTopologyRunner {   
	
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
      
		String boltId = "";
		String prevId;
		
		/*
		 * Modification begin
		 */
		boltId = "source";
		builder.setSpout(boltId,new JsonSpout());// -> "tweet"
		prevId = boltId;

		boltId = "totext";
		builder.setBolt(boltId, new TweetJsonToTextBolt()).shuffleGrouping(prevId); // "tweet" -> "words"
		prevId = boltId;
		
		boltId = "ngramer";
		builder.setBolt(boltId, new NGramerBolt()).shuffleGrouping(prevId); // "words" -> "n-gram"
		prevId = boltId;
		
		/* 
		 * Inline definition of the file output bolt
		 */
		boltId = "file";
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(10000);
        // rotate files when they reach 1MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1, FileSizeRotationPolicy.Units.MB);
        // where to save files and how should they be called
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/home/martijn/data/").withExtension(".txt");
        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
		HdfsBolt bolt = new HdfsBolt()
        .withFsUrl("file://home/martijn/data")
        .withFileNameFormat(fileNameFormat)
        .withRecordFormat(format)
        .withRotationPolicy(rotationPolicy)
        .withSyncPolicy(syncPolicy)
        .addRotationAction(new MoveFileAction().toDestination("/home/martijn/data")); // move old files 
		
		builder.setBolt(boltId, bolt).shuffleGrouping(prevId);
		prevId = boltId;
		
		
		/*
		 * Modification end
		 */
		StormTopology topology = builder.createTopology();
		return topology;
	}
	
    
    public static void main(String[] args) {
    	
    	String[] a = new String[2];
    	a[0] = "Topology41";
    	a[1] = "local";
    	
    	new Assignment4_1().run(a);
    }
}
