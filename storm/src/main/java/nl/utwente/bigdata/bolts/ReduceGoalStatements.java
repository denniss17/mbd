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
import nl.utwente.bigdata.util.ReportedScores;
import nl.utwente.bigdata.util.Score;
import nl.utwente.bigdata.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ReduceGoalStatements extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private static final int threshold = 30; // amount of tweets per minute for bolt to trigger

	private Map<Match, ReportedScores> scoreMap;
	private Map<Match, Score> prevScoreMap;

	private Date prevEmission = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		scoreMap = new HashMap<Match, ReportedScores>();
		prevScoreMap = new HashMap<Match, Score>();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Match match = (Match) tuple.getValueByField("match");
		if (match == null)
			return;

		Date date = (Date) tuple.getValueByField("time");
		if (date == null)
			return;

		Score score = (Score) tuple.getValueByField("score");
		if (score == null)
			return;

		if (prevEmission != null) {
			/**
			 * Simply append the data to the data structures
			 */
			if (scoreMap.containsKey(match)) {
				scoreMap.get(match).insertScore(score);
			} else {
				scoreMap.put(match, new ReportedScores(match.hashtag, threshold));
			}

			/**
			 * Execute every minute in world cup time
			 */
			if ((date.getTime() - prevEmission.getTime()) / 1000 > 60) {
				// Check all matches that have scores associated with them
				for (Map.Entry<Match, ReportedScores> entry : scoreMap.entrySet()) {
					Match m = entry.getKey();
					ReportedScores rs = entry.getValue();

					Score mostReportedScore = rs.getMostMentionedScore();

					if (mostReportedScore != null) {
						// If there is a score which has been reported more than
						// the threshold

						// Either, the match has not yet occured in the map
						// OR the found score in map is unequal to the
						// mostReportedScore
						if (!prevScoreMap.containsKey(m)) 
						{
							collector.emit(new Values(date, m, mostReportedScore));
							prevScoreMap.put(m, mostReportedScore);
						} 
						else 
						{
							if (!prevScoreMap.get(m).equals(mostReportedScore)) 
							{
								collector.emit(new Values(date, m, mostReportedScore));
								prevScoreMap.put(m, mostReportedScore);
							}
						}
					}
				}

				scoreMap.clear();
				prevEmission = date;
			}
		} else {
			prevEmission = date;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "match", "score"));
	}
}