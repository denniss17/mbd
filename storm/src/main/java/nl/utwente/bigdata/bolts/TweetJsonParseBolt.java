package nl.utwente.bigdata.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.log.LogLevel;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TweetJsonParseBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1605773792988518130L;
	private static Logger logger = Logger.getLogger(TweetJsonParseBolt.class
			.getName());
	private transient JSONParser parser;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		parser = new JSONParser();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			JSONObject tweet = (JSONObject) parser.parse(tuple.getString(0));
			JSONObject entities = (JSONObject) tweet.get("entities");
			JSONArray hashtagArray = (JSONArray) entities.get("hashtags");

			String text = (String) tweet.get("text");
			String lang = (String) tweet.get("lang");
			String time = (String) tweet.get("created_at");

			List<String> hashtags = new ArrayList<String>();
			for (int i = 0; i < hashtagArray.size(); i++) {
				JSONObject hashtag = (JSONObject) hashtagArray.get(i);
				String hashtagText = (String) hashtag.get("text");

				if (hashtagText != null) {
					hashtags.add(hashtagText);
				}
			}
			
			collector.emit(new Values(text, lang, time, hashtags));
		} catch (ClassCastException e) {
			logger.info(e.toString());
			return; // do nothing (we might log this)
		} catch (org.json.simple.parser.ParseException e) {
			System.out.println("ParseException");
			e.printStackTrace();
			return; // do nothing
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("text", "lang", "time", "hashtags"));
	}

}
