package nl.utwente.bigdata.bolts;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.utwente.bigdata.util.Match;
import nl.utwente.bigdata.util.Score;
import nl.utwente.bigdata.util.WorldCupReader;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExtractGoalFromTweetData extends BaseBasicBolt {
	private static final long serialVersionUID = -2632529340918678149L;
	public static final Pattern scorePattern = Pattern.compile("(\\d+)[-:](\\d+)");
	private static Logger logger = Logger.getLogger(ExtractGoalFromTweetData.class.getName());

	private Map<String, Match> matches;
	private Set<String> matchHashtags;

	private Matcher matcher;

	// Thu Jul 03 05:17:20 +0000 2014
	private static final String TIME_FORMAT = "EEE MMM dd HH:mm:ss Z yyyy";

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			String text = tuple.getStringByField("text");
			String lang = tuple.getStringByField("lang");
			Date time = this.parseTime(tuple.getStringByField("time"));
			List<String> hashtags = (List<String>) tuple.getValueByField("hashtags");

			this.checkTweet(text, lang, time, hashtags, collector);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Date parseTime(String date) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat(TIME_FORMAT);
		return formatter.parse(date);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);

		this.matches = WorldCupReader.getInstance().getMatches();

		// For the sake of speed, keep the keyset too
		this.matchHashtags = matches.keySet();

		logger.info(matchHashtags.toString());
	}

	private void checkTweet(String text, String lang, Date time, List<String> hashtags, BasicOutputCollector collector) {
		if (text == null || lang == null) {
			return;
		}

		// Check for matchhashtags
		for (String hashtag : hashtags) {
			if (matchHashtags.contains(hashtag.toUpperCase())) {
				// Tweet about a match

				// Check for scores
				matcher = scorePattern.matcher(text);

				if (matcher.find()) {
					// Score found
					try {
						Match match = matches.get(hashtag);

						
						int homeGoals = Integer.parseInt(matcher.group(1));
						int awayGoals = Integer.parseInt(matcher.group(2));

						if(homeGoals < 10 && awayGoals < 10) {
							Score score = new Score(homeGoals, awayGoals);
							collector.emit(new Values(time, match, score));
						}
						
					} catch (NumberFormatException e) {
						// Should not occur because of regex
					}
				}

			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "match", "score"));
	}
}
