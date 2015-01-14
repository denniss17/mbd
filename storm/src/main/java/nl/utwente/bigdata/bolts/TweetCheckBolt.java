package nl.utwente.bigdata.bolts;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public abstract class TweetCheckBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2299961857911526321L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			String text = tuple.getStringByField("text");
			String lang = tuple.getStringByField("lang");
			String time = tuple.getStringByField("time");
			List<String> hashtags = (List<String>) tuple
					.getValueByField("hashtags");

			this.checkTweet(text, lang, time, hashtags, collector);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected abstract void checkTweet(String text, String lang, String time,
			List<String> hashtags, BasicOutputCollector collector);
}
