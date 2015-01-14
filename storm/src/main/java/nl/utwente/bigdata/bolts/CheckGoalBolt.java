package nl.utwente.bigdata.bolts;

import java.util.List;

import nl.utwente.bigdata.Match;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CheckGoalBolt extends TweetCheckBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2632529340918678149L;
	private List<Match> matches;

	public CheckGoalBolt(List<Match> matches) {
		this.matches = matches;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "country"));
	}

	@Override
	protected void checkTweet(String text, String lang, String time,
			List<String> hashtags, BasicOutputCollector collector) {
		if (text == null || lang == null) {
			return;
		}
		
		

		//System.out.println(text.length() + "\t" + lang + "\t" + hashtags);
		collector.emit(new Values(time, lang));
	}
}
