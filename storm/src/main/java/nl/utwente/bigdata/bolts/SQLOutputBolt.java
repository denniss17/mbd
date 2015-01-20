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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SQLOutputBolt extends BaseBasicBolt {
	
	private static final String SERVER_URL = "http://mbd.dennisschroer.nl/database_upload.php";

	private static final long serialVersionUID = -4036021649003516880L;

	private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// fields in tuple: "time":Date, "hashtag":String, "homeCountry":String, "awayCountry":String, "homeScore":int, "awayScore":int
		
		Date time = (Date) tuple.getValueByField("time");
		
		StringBuilder builder = new StringBuilder(TIME_FORMAT);
		builder.append(SERVER_URL);
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		builder.append("?time=" + dateFormat.format(time));
		builder.append("&matchhash=" + tuple.getStringByField("hashtag"));
		builder.append("&country1=" + tuple.getStringByField("homeCountry"));
		builder.append("&country2=" + tuple.getStringByField("awayCountry"));
		builder.append("&score=" + tuple.getIntegerByField("homeScore") + "-" + tuple.getIntegerByField("awayScore"));
		
		collector.emit(new Values(builder.toString()));
		
		//System.out.println(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("url"));
	}

}
