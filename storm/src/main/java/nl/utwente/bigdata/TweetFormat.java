package nl.utwente.bigdata;

import static backtype.storm.utils.Utils.tuple;
import static java.util.Arrays.asList;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.tuple.Fields;

public final class TweetFormat implements MultiScheme, Serializable {
	private static final long serialVersionUID = -2223610007439801748L;

	@Override
	public Fields getOutputFields() {
		return new Fields("tweet");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterable<List<Object>> deserialize(byte[] ser) {
		try {
			return asList(tuple(new String(ser, "UTF-8")));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return asList(tuple(""));
	}
}