package nl.utwente.bigdata.bolts;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.utwente.bigdata.Match;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CheckGoalBolt extends TweetCheckBolt {
	private static final long serialVersionUID = -2632529340918678149L;
	
	private Map<String, Match> matches;
	private Set<String> matchHashtags;

	public CheckGoalBolt(Map<String, Match> matches) {
		this.matches = matches;
		// For the sake of speed, keep the keyset too
		this.matchHashtags = matches.keySet();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "country"));
	}

	@Override
	protected void checkTweet(String text, String lang, Date time,
			List<String> hashtags, BasicOutputCollector collector) {
		if (text == null || lang == null) {
			return;
		}
		
		for(String hashtag : hashtags){
			if(matchHashtags.contains(hashtag)){
				// Tweet about a match
				Match match = matches.get(hashtag);
				
			}
		}		
	}
}
