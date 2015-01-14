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
import java.util.Arrays;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NGramerBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4031901444200770796L;
	private static final long N = 2;
	private long size = 0;	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
			/*
			 * Get the tweet text from the tuple
			 */
			String text = (String) tuple.getStringByField("words");
			
			/*
			 * Split the tweet text into a list based on the delimiter
			 */
			List<String> list = new ArrayList<String>(Arrays.asList(text.split("\\s")));
			
			/*
			 * Emit all the Ngrams
			 */
			for(int i = 0; i<list.size() - N + 1; i++) {
				List<String> sublist = new ArrayList<String>(list.subList(i,(int) (i+N)));
				collector.emit(new Values(sublist));
				size += sublist.toString().length();
				System.out.println(size);
			}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("n-gram"));
	}

}
