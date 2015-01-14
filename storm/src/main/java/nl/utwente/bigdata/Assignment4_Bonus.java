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

import nl.utwente.bigdata.bolts.MovingAvgBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.spouts.RandomNumberBolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class Assignment4_Bonus extends AbstractTopologyRunner {   
	
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();
      
		String boltId = "";
		String prevId;
		
		boltId = "spout";
		builder.setSpout(boltId, new RandomNumberBolt());
		prevId = boltId;
		
		// TODO: add topology generation for assignment 4.3 here: Add the 
		// completed MovingAvgBolt
		boltId = "movingavg"; 
		builder.setBolt(boltId, new MovingAvgBolt()).shuffleGrouping(prevId); 
		prevId = boltId;
		
		boltId = "printer"; 
		builder.setBolt(boltId, new PrinterBolt()).shuffleGrouping(prevId); 
		prevId = boltId;
		
		StormTopology topology = builder.createTopology();
		return topology;
		        
	}
	
    
    public static void main(String[] args) {
    	new Assignment4_Bonus().run(args);;
    }
}
