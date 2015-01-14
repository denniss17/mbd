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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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

public class TopCounterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private static int K = 5;
	
	private Map<String, Integer> counter = new HashMap<String, Integer>();
	
	public TopCounterBolt(int top) {
		super();
		K = top;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		if (TupleHelpers.isTickTuple(tuple)) 
		{
			// Map -> sorted LinkedHashMap -> sorted List
			List<Entry<String,Integer>>  list= new ArrayList<Entry<String,Integer>>(sortByValues(counter).entrySet());
			
			for(int i = 0; i<K; i++) {
				Integer ival = list.get(i).getValue();
				String sval = list.get(i).getKey();
				collector.emit(new Values(sval, ival));
			}
			
			counter.clear();
		}
		else 
		{
			String word = tuple.getStringByField("word");
		
			if(counter.containsKey(word)) {
				Integer i = counter.get(word);
				counter.put(word,i+1);
			} 
			else 
			{
				counter.put(word, 1);
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private <K extends Comparable, V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
		List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
        
		Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {
            @SuppressWarnings("unchecked")
			@Override
            public int compare(Entry<K, V> o1, Entry<K, V> o2) { return o2.getValue().compareTo(o1.getValue()); }
        });
     
        Map<K,V> sortedMap = new LinkedHashMap<K,V>();
     
        for(Map.Entry<K,V> entry: entries) { sortedMap.put(entry.getKey(), entry.getValue()); }
     
        return sortedMap;
    }
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120);
		return conf;
	}
}
