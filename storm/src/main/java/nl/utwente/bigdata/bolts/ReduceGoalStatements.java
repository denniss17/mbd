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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import nl.utwente.bigdata.util.Match;
import nl.utwente.bigdata.util.Score;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ReduceGoalStatements extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private Map<String, Score> scoreMap;
	private Map<String, Score> possibleScoreMap;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		scoreMap = new HashMap<String, Score>();
		possibleScoreMap = new HashMap<String, Score>();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Match match = (Match) tuple.getValueByField("match");
		if(match == null) return;
		
		Date date = (Date) tuple.getValueByField("time");
		if(date == null) return;
		
		Score score = (Score) tuple.getValueByField("score");
		if(score == null) return;
		
		String teams;
		if (match.hashtag != null) {
			teams = match.hashtag;
		} else {
			return;
		}

		if (scoreMap.containsKey(teams)) {
			if (score.equals(scoreMap.get(teams))) {
				// when the same score has been tweeted again
				// do nothing
			} else {
				// a new score has been reported

				if (score.equals(possibleScoreMap.get(teams))) {
					// if the score was already a possible score
					// update the score in the real score map
					// emit the score update
					scoreMap.put(teams, score);
					collector.emit(new Values(date, match, score));
				} else {
					// enter the possible score
					possibleScoreMap.put(teams, score);
				}
			}
		} else {
			// Teams never seen before
			scoreMap.put(teams, score);
			possibleScoreMap.put(teams, score);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "match", "score"));
	}
}
/*
 * if(score.equals(scoreMap.get(teams))) { //Okay, same score might have been
 * reported again } else { //Score that was not seen yet
 * 
 * 
 * if(possibleScoreMap.containsKey(teams)) { //Score was already entered as
 * possible score
 * 
 * 
 * } else { //Enter it as a possible score possibleScoreMap.put(teams, score); }
 * }
 */