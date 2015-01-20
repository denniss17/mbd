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
package nl.utwente.bigdata.bolts.old;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MovingAvgBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private static final int W = 1000;
	
	private ArrayList<Double> vals = new ArrayList<Double>(100);
	private double avg = 0;
	private boolean bufferFilled = false;
	private int index = 0;
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		double d = tuple.getDoubleByField("number");

		/*
		 *  Recompute the average every W times (to prevent floating point error accumulation)
		 */
		if(index == W) { index = 0; bufferFilled = true; avg = average(vals); }
		
		if(bufferFilled) {
			/*
			 * Update the moving average, for speed don't recompute the entire average but use math trick.
			 */
			avg = avg + (d - vals.get(index))/W;
			vals.set(index, d);
			collector.emit(new Values(avg));
		} 
		else {
			vals.add(d);
		}

		index++;
	}
	
	private double average(List<Double> l) {
		if(l.size() == 0) { return 0; }
		
		double sum = 0;
		for(Double d : l) { sum += d; }
		return sum/l.size();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("average"));
	}
}
