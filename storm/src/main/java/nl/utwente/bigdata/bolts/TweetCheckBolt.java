package nl.utwente.bigdata.bolts;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public abstract class TweetCheckBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2299961857911526321L;
	// Thu Jul 03 05:17:20 +0000 2014
	private static final String TIME_FORMAT = "EEE MMM dd HH:mm:ss Z yyyy";

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			String text = tuple.getStringByField("text");
			String lang = tuple.getStringByField("lang");
			Date time = this.parseTime(tuple.getStringByField("time"));
			List<String> hashtags = (List<String>) tuple
					.getValueByField("hashtags");

			this.checkTweet(text, lang, time, hashtags, collector);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Date parseTime(String date) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat(TIME_FORMAT);
        return formatter.parse(date);
	}

	protected abstract void checkTweet(String text, String lang, Date time,
			List<String> hashtags, BasicOutputCollector collector);
}
