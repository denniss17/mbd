package nl.utwente.bigdata;

import java.util.List;
import java.util.Properties;

import nl.utwente.bigdata.bolts.CheckGoalBolt;
import nl.utwente.bigdata.bolts.CountGoalBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.bolts.TweetJsonParseBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class GoalDetector extends AbstractTopologyRunner {

	/** The list of scheduled mathces of the WorldCup */
	private List<Match> matches;

	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();

		String boltId = "";
		String prevId;

		// Set up the kafka spout
		boltId = "kafka";
		SpoutConfig spoutConf = new SpoutConfig(new ZkHosts(
				properties.getProperty("zkhost", "localhost:2181")),
				properties.getProperty("topic", "worldcup"), "/brokers",
				"worldcup");

		spoutConf.forceFromStart = true;
		spoutConf.scheme = new TweetFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);

		// Add the kafka spout
		builder.setSpout(boltId, spout);
		prevId = boltId;

		// Parse the tweet
		boltId = "parser";
		builder.setBolt(boltId, new TweetJsonParseBolt()).shuffleGrouping(
				prevId);
		prevId = boltId;

		// Extract goals
		// "tweet" -> "country"
		builder.setBolt(boltId, new CheckGoalBolt(matches)).shuffleGrouping(prevId);
		prevId = boltId;

		// Count the country occurrences
		// "time", "country" -> "time", "timespan", "country", "amount"
		boltId = "counter";
		builder.setBolt(boltId, new CountGoalBolt()).fieldsGrouping(prevId,
				new Fields("country"));
		prevId = boltId;

		//
		// boltId = "topcounter";
		// builder.setBolt(boltId, new
		// TopCounterBolt(25)).fieldsGrouping(prevId, new Fields("word")); //
		// "word" -> "word", "count"
		// prevId = boltId;

		/*
		 * OUTPUT 1: hdfs
		 */
		// boltId = "file";
		// SyncPolicy syncPolicy = new CountSyncPolicy(1000);
		// FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1,
		// FileSizeRotationPolicy.Units.KB); // rotate files when they reach 1KB
		// FileNameFormat fileNameFormat = new
		// DefaultFileNameFormat().withPath("/user/martijn/tweets/").withExtension(".txt");
		// RecordFormat format = new
		// DelimitedRecordFormat().withFieldDelimiter("|"); // use "|" instead
		// of "," for field delimiter
		//
		// HdfsBolt bolt = new HdfsBolt()
		// .withFsUrl("hdfs://localhost:8020")
		// .withFileNameFormat(fileNameFormat)
		// .withRecordFormat(format)
		// .withRotationPolicy(rotationPolicy)
		// .withSyncPolicy(syncPolicy)
		// .addRotationAction(new
		// MoveFileAction().toDestination("/user/martijn/old/"));
		//
		// builder.setBolt(boltId, bolt).shuffleGrouping(prevId);

		/*
		 * OUTPUT 2: standard out
		 */
		boltId = "print";
		builder.setBolt(boltId, new PrinterBolt()).shuffleGrouping(prevId);

		StormTopology topology = builder.createTopology();
		return topology;

	}

	public static void main(String[] args) {
		if (args.length < 1 || (!args[0].equals("local") && !args[0].equals("cluster"))) {
			System.out.println("Usage: storm <>.jar nl.utwente.bigdata.GoalDetector local|cluster");
		}else{
			String[] a = new String[2];
			a[0] = "ScoreSummarizer";
			a[1] = args[0];
			
			GoalDetector goalDetector = new GoalDetector();
			goalDetector.setMatches(WorldCupReader.getInstance().getMatches());
			goalDetector.run(a);
		}

		
	}

	private void setMatches(List<Match> matches) {
		this.matches = matches;
	}
}
