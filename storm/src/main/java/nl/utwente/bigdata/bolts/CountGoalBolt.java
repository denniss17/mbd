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
package nl.utwente.bigdata.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import nl.utwente.bigdata.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountGoalBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private static final Integer measurePeriod = 1;
	
	private Map<String, Integer> counter = new HashMap<String, Integer>();
	private String mostRecentTime = "";
	
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) 
	{
		if (TupleHelpers.isTickTuple(tuple)) 
		{
			for (Entry<String, Integer> entry : counter.entrySet()) 
			{
				Values v = new Values(
					mostRecentTime, 
					measurePeriod.toString(),
					entry.getKey(), 
					entry.getValue()
				);
				collector.emit(v);				
			}
		} 
		else 
		{
			String country = tuple.getStringByField("country");
			mostRecentTime = tuple.getStringByField("time");
			
			Integer amount = counter.get(country);
			if(amount == null) {
				counter.put(country, 1);
			} else {
				counter.put(country, amount + 1);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "timespan", "country", "amount"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, measurePeriod);
		return conf;
	}
}
