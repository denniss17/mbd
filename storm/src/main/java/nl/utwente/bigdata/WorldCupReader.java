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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * This class reads worldcup info like played matches and returns it on request
 */
public class WorldCupReader {
	private static WorldCupReader instance;

	private List<Match> matches;
	private transient JSONParser parser;

	public static WorldCupReader getInstance() {
		if (instance == null)
			instance = new WorldCupReader();
		return instance;
	}

	private void WorldCupReader() {
		this.matches = new ArrayList<Match>();

		// Read data on startup
		try {
			StringBuilder stringBuilder = new StringBuilder();
			parser = new JSONParser();

			// Read content of json file
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					getClass().getResourceAsStream("worldcup-games.json")));
			String line = null;
			while ((line = reader.readLine()) != null) {
				stringBuilder.append(line);
			}
			reader.close();

			// Parse the json
			JSONArray data = (JSONArray) parser.parse(stringBuilder.toString());
			for (Object m : data) {
				JSONObject match = (JSONObject) m;

				JSONObject home = (JSONObject) match.get("home");
				JSONObject away = (JSONObject) match.get("away");

				Match result = new Match();

				result.homeCountry = (String) home.get("name");
				result.awayCountry = (String) away.get("name");
				result.start = (String) match.get("time");
				this.matches.add(result);
				System.out.println(result);
			}
			
			// Sort on time
			Collections.sort(this.matches);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the scheduled matches of the World Cup
	 * @return a list of Matches
	 */
	public List<Match> getMatches() {
		return matches;
	}
}