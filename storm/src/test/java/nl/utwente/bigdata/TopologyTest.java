package nl.utwente.bigdata;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

public class TopologyTest {

	/*
	@Test
	public void test() throws IOException {
		Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("twitterrc"));
        properties.put("sleep", String.valueOf(60 * 1000));
        PrintTwitter topology = new PrintTwitter();
		topology.runLocal("test", properties);
	}
	*/
	
	@Test
	public void testRandomTweet() {
		Properties properties = new Properties();
		SaveRandomTweets topology = new SaveRandomTweets();
		topology.runLocal("test", properties);
	}
	
	@Test
	public void testRandomTokenziedTweet() {
		Properties properties = new Properties();
		PrintRandomTweetTokens topology = new PrintRandomTweetTokens();
		topology.runLocal("test", properties);
	}
	
	@Test
	public void testAssignment4_3() {
		Properties properties = new Properties();
		AbstractTopologyRunner runner= new Assignment4_3();
		runner.runLocal("test", properties);
	}
}
