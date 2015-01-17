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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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

	private WorldCupReader() {}
	
	public void load() {
		try {
			JSONArray data = loadData();
			matches = loadMatches(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassCastException e){
			// Some JSON was incorrectly casted
			e.printStackTrace();
		}
		
	}
	
	String loadRawData() throws IOException{
		StringBuilder stringBuilder = new StringBuilder();
		
		// Read content of json file
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				this.getClass().getResourceAsStream("worldcup-games.json")));
		String line = null;
		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
		}
		reader.close();
		
		return stringBuilder.toString();
	}

	private JSONArray loadData() throws IOException, ParseException{
		// Load data
		String rawData = loadRawData();
		
		// Parse the json
		parser = new JSONParser();
		JSONArray data = (JSONArray) parser.parse(rawData);
		
		return data;
	}
	
	private List<Match> loadMatches(JSONArray data){
		List<Match> result = new ArrayList<Match>();

		// Read data on startup
		for (Object m : data) {
			JSONObject matchObject = (JSONObject) m;

			JSONObject home = (JSONObject) matchObject.get("home");
			JSONObject away = (JSONObject) matchObject.get("away");

			Match match = new Match();

			match.homeCountry = (String) home.get("name");
			match.awayCountry = (String) away.get("name");
			match.start = (String) matchObject.get("time");
			result.add(match);
		}
		
		// Sort on time
		Collections.sort(result);
		
		return result;
	}

	/**
	 * Get the scheduled matches of the World Cup
	 * @return a list of Matches or null if the loading failed
	 */
	public List<Match> getMatches() {
		// Lazy loading
		if(this.matches == null) {
			this.load();
		}
		return this.matches;
	}
}