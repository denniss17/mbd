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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import nl.utwente.bigdata.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TopCounterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private static final long K = 50;
	
	private Map<String, Integer> counter = new HashMap<String, Integer>();
	private ValueComparator bvc =  new ValueComparator(counter);
	private TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(bvc);
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			// this was a chronological tickle tuple - generate output
			
			// Sort by value
			sortedMap.putAll(counter);
			
			// Emit K results
			int count = 0;
			for(Map.Entry<String, Integer> entry : sortedMap.entrySet()){
				if(count<K){
					collector.emit(new Values(entry.getKey(), entry.getValue()));
					count++;
				}
			}
		} else {
			// Increase counter
			String token = tuple.getString(0);
			Integer count = counter.get(token);
			if (count == null) {
				count = 0;
			}
			count = count + 1;
			counter.put(token, count);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120);
		return conf;
	} 
	
	// Comperator which sorts a TreeMap by value
	// Source: http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java
	class ValueComparator implements Comparator<String> {

	    Map<String, Integer> base;
	    public ValueComparator(Map<String, Integer> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(String a, String b) {
	        if (base.get(a) >= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}
}
