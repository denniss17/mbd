package nl.utwente.bigdata;

import java.util.Properties;

import nl.utwente.bigdata.bolts.ExtractDataFromTweetJSON;
import nl.utwente.bigdata.bolts.ExtractGoalFromTweetData;
import nl.utwente.bigdata.bolts.ReduceGoalStatements;
/*
 import org.apache.storm.hdfs.bolt.HdfsBolt;
 import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
 import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
 import org.apache.storm.hdfs.bolt.format.FileNameFormat;
 import org.apache.storm.hdfs.bolt.format.RecordFormat;
 import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
 import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
 import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
 import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
 import org.apache.storm.hdfs.common.rotation.MoveFileAction;
 */
import nl.utwente.bigdata.outputbolts.PrinterBolt;
import nl.utwente.bigdata.outputbolts.SQLOutputBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class GoalDetector extends AbstractTopologyRunner {

	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();

		String boltId = "";
		String prevId;

		// Set up the kafka spout
		boltId = "kafka";
		SpoutConfig spoutConf = new SpoutConfig(new ZkHosts(properties.getProperty("zkhost", "localhost:2181"),
				"/brokers"), properties.getProperty("topic", "worldcup3"), "/kafka", "worldcup3");

		spoutConf.forceFromStart = true;
		spoutConf.scheme = new TweetFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);

		// Add the kafka spout
		// "line"
		builder.setSpout(boltId, spout);
		prevId = boltId;

		// Parse the tweet
		// "text", "lang", "time", "hashtags"
		boltId = "parser";
		builder.setBolt(boltId, new ExtractDataFromTweetJSON()).shuffleGrouping(prevId);
		prevId = boltId;

		// Extract goals
		// "time":Date, "match":Match, "score":Score
		boltId = "checkgoal";
		builder.setBolt(boltId, new ExtractGoalFromTweetData()).shuffleGrouping(prevId);
		prevId = boltId;

		
		// Emit whenever the score changes
		// "time":Date, "match":Match, "score":Score
		boltId = "summarizer";
		builder.setBolt(boltId, new ReduceGoalStatements()).shuffleGrouping(prevId);
		prevId = boltId;
		
		
		/*
		 * OUTPUT 0: SQL
		 */
		boltId = "sqloutput";
		builder.setBolt(boltId, new SQLOutputBolt()).shuffleGrouping(prevId);

		/*
		 * OUTPUT 1: standard out
		 */
		boltId = "print";
		builder.setBolt(boltId, new PrinterBolt()).globalGrouping(prevId);

		StormTopology topology = builder.createTopology();
		return topology;
	}

	public static void main(String[] args) {

		if (args.length < 1 || (!args[0].equals("local") && !args[0].equals("cluster"))) {
			System.out.println("Usage: storm <>.jar nl.utwente.bigdata.GoalDetector local|cluster");
		} else {
			String[] a = new String[2];
			a[0] = "GoalDetector";
			a[1] = args[0];

			GoalDetector goalDetector = new GoalDetector();
			goalDetector.run(a);
		}
	}
}
