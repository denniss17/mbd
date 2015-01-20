package nl.utwente.bigdata.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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

public class ExtractDataFromTweetJSON extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1605773792988518130L;
	private static Logger logger = Logger.getLogger(ExtractDataFromTweetJSON.class.getName());
	private transient JSONParser parser;

	@SuppressWarnings("rawtypes")
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
			logger.info(e.toString());
			e.printStackTrace();
			return; // do nothing
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("text", "lang", "time", "hashtags"));
	}

}
