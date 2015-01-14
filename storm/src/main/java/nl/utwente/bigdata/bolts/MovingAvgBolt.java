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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;

import com.google.common.collect.ImmutableList;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MovingAvgBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private static final int W = 100;
	
	private double average;
	private Queue<Double> queue = new ArrayDeque<Double>();
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// Get number and add to queue
		double number = tuple.getDouble(0);
		queue.add(number);
		
		if(queue.size() == W){
			// Calculate average
			double sum = 0;
			for(double d : queue){
				sum += d;
			}
			average = sum / W;
			collector.emit(new Values(average));
		}else if(queue.size() == W+1){
			// Recalculate the average by removing the first and adding the last
			// This could be not accurate (when numbers get rounded)
			average -= queue.poll() / W;
			average += number / W;
			collector.emit(new Values(average));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("average"));
	}
}
