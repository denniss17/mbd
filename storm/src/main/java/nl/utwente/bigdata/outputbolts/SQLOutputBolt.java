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
package nl.utwente.bigdata.outputbolts;

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import nl.utwente.bigdata.util.Match;
import nl.utwente.bigdata.util.Score;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SQLOutputBolt extends BaseBasicBolt {

	private static final String SERVER_URL = "http://mbd.dennisschroer.nl/database_upload.php";
	private static final long serialVersionUID = -4036021649003516880L;
	private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final TimeZone TIMEZONE = TimeZone.getTimeZone("GMT");

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		Date time = (Date) tuple.getValueByField("time");
		Match match = (Match) tuple.getValueByField("match");
		Score score = (Score) tuple.getValueByField("score");
		
		StringBuilder builder = new StringBuilder();
		SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_FORMAT);
		dateFormat.setTimeZone(TIMEZONE);

		builder.append(SERVER_URL);
		builder.append("?time=" + dateFormat.format(time));
		builder.append("&matchhash=" + match.hashtag);
		builder.append("&country1=" + match.homeCountry.toLowerCase());
		builder.append("&country2=" + match.awayCountry.toLowerCase());
		builder.append("&score=" + score.T1goals + "-" + score.T2goals);

		this.executeHTTPGet(builder.toString().replace(" ", "%20"));
	}

	private void executeHTTPGet(String url2) {
		URL url;
		HttpURLConnection connection = null;
		try {
			// Create connection
			url = new URL(url2);
			connection = (HttpURLConnection) url.openConnection();
			connection.getInputStream();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}
}
