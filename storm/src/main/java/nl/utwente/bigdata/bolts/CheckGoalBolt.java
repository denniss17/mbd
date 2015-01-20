package nl.utwente.bigdata.bolts;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.utwente.bigdata.Match;
import nl.utwente.bigdata.WorldCupReader;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CheckGoalBolt extends TweetCheckBolt {
	private static final long serialVersionUID = -2632529340918678149L;

	public static final Pattern scorePattern = Pattern.compile("(\\d+)[-:](\\d+)");
	
	private static Logger logger = Logger.getLogger(CheckGoalBolt.class
			.getName());
	
	private Map<String, Match> matches;
	private Set<String> matchHashtags;
	
	private Matcher matcher;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		
		this.matches = WorldCupReader.getInstance().getMatches();
		
		// For the sake of speed, keep the keyset too
		this.matchHashtags = matches.keySet();
		
		logger.info(matchHashtags.toString());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "hashtag", "homeCountry", "awayCountry", "homeScore", "awayScore"));
	}

	@Override
	protected void checkTweet(String text, String lang, Date time,
			List<String> hashtags, BasicOutputCollector collector) {
		if (text == null || lang == null) {
			return;
		}
		
		// Check for matchhashtags
		for(String hashtag : hashtags){
			if(matchHashtags.contains(hashtag.toUpperCase())){
				// Tweet about a match
				
				// Check for scores
				matcher = scorePattern.matcher(text);
				
				if(matcher.find()){
					// Score found
					try{
						// group 0 is the entire score
						// group 1 is the first number
						// group 2 is the second number
						int homeGoals = Integer.parseInt(matcher.group(1));
						int awayGoals = Integer.parseInt(matcher.group(2));
						
						// Compare the score with the known last score
						// Get the match
						Match match = matches.get(hashtag);
						
						// System.out.println(hashtag);
						collector.emit(new Values(time, match.hashtag, match.homeCountry, match.awayCountry, homeGoals, awayGoals));
						
						/*if(homeGoals > match.homeGoals){
							// It looks like the home country scored
							collector.emit(new Values(time, match.homeCountry));
						}
						
						if(awayGoals > match.awayGoals){
							// It looks like the home country scored
							collector.emit(new Values(time, match.awayCountry));
						}*/
						
					}catch(NumberFormatException e){
						// Should not occur because of regex
					}
				}
			}
		}		
	}
}
