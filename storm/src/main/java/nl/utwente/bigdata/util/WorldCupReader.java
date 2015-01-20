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
package nl.utwente.bigdata.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * This class reads worldcup info like played matches and returns it on request
 */
public class WorldCupReader {
	private static WorldCupReader instance;

	private Map<String, Match> matches;
	private transient JSONParser parser;

	// 2014-06-12 17:00:00
	// private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss ZZZZ";

	public static WorldCupReader getInstance() {
		if (instance == null)
			instance = new WorldCupReader();
		return instance;
	}

	private WorldCupReader() {
	}

	/**
	 * Load the data from the JSON file and parse it
	 */
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
		} catch (ClassCastException e) {
			// Some JSON was incorrectly casted
			e.printStackTrace();
		}

	}

	/**
	 * Read the raw content from the JSON file
	 * 
	 * @return String the content of the file
	 * @throws IOException
	 */
	public String loadRawData() throws IOException {
		StringBuilder stringBuilder = new StringBuilder();

		// Read content of json file
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(this.getClass().getResourceAsStream("worldcup-games.json")));
		String line = null;
		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
		}
		reader.close();

		return stringBuilder.toString();
	}

	/**
	 * Read the content from the JSON file and parse it
	 * 
	 * @return A JSONArray containing JSONObjects
	 * @throws IOException
	 * @throws ParseException
	 */
	private JSONArray loadData() throws IOException, ParseException {
		// Load data
		String rawData = loadRawData();

		// Parse the json
		parser = new JSONParser();
		JSONArray data = (JSONArray) parser.parse(rawData);

		return data;
	}

	private Map<String, Match> loadMatches(JSONArray data) {
		Map<String, Match> result = new HashMap<String, Match>();

		// Read data on startup
		for (Object m : data) {
			JSONObject matchObject = (JSONObject) m;

			JSONObject home = (JSONObject) matchObject.get("home");
			JSONObject away = (JSONObject) matchObject.get("away");

			Match match = new Match();

			match.homeCountry = (String) home.get("name");
			match.awayCountry = (String) away.get("name");

			String homeTag = CountryHashtags.get(match.homeCountry);
			String awayTag = CountryHashtags.get(match.awayCountry);

			// For testing purposes : throw exception if a tag is not defined
			// This will cause the build to fail
			if (homeTag == null) {
				// Ugly but works
				throw new RuntimeException("No tag found for " + match.homeCountry);
			}
			if (awayTag == null) {
				throw new RuntimeException("No tag found for " + match.awayCountry);
			}

			String hashtag = CountryHashtags.get(match.homeCountry) + CountryHashtags.get(match.awayCountry);

			match.hashtag = hashtag;

			/*
			 * try { match.start = this.parseTime((String)
			 * matchObject.get("time")); } catch (java.text.ParseException e) {
			 * // TODO Auto-generated catch block e.printStackTrace(); }
			 */
			result.put(hashtag, match);
		}

		return result;
	}
	/*
	private Date parseTime(String date) throws java.text.ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat(TIME_FORMAT);
		return formatter.parse(date + " -0200");
	}*/

	/**
	 * Get the scheduled matches of the World Cup
	 * 
	 * @return a list of Matches or null if the loading failed
	 */
	public Map<String, Match> getMatches() {
		// Lazy loading
		if (this.matches == null) {
			this.load();
		}
		return this.matches;
	}
}