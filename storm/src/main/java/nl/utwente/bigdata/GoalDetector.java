package nl.utwente.bigdata;

import java.util.Properties;

import nl.utwente.bigdata.bolts.ExtractDataFromTweetJSON;
import nl.utwente.bigdata.bolts.ExtractGoalFromTweetData;
import nl.utwente.bigdata.bolts.ReduceGoalStatements;
import nl.utwente.bigdata.outputbolts.PrinterBolt;
import nl.utwente.bigdata.outputbolts.SQLOutputBolt;

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

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class GoalDetector extends AbstractTopologyRunner {
	/**
	 * The session id of this run
	 */
	public static int session;

	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();

		String boltId = "kafka";
		String prevId;

		// Set up the kafka spout
		this.setupKafkaSpout(boltId, builder, properties);

		prevId = boltId;

		// Parse the tweet
		// "text", "lang", "time", "hashtags"
		boltId = "parser";
		builder.setBolt(boltId, new ExtractDataFromTweetJSON())
				.shuffleGrouping(prevId);
		prevId = boltId;

		// Extract goals
		// "time":Date, "match":Match, "score":Score
		boltId = "checkgoal";
		builder.setBolt(boltId, new ExtractGoalFromTweetData())
				.shuffleGrouping(prevId);
		prevId = boltId;

		// Emit whenever the score changes
		// "time":Date, "match":Match, "score":Score
		boltId = "summarizer";
		// Group my match hashtag to guarantee that the scores of the same matches
		// are reduced by the same bolt
		builder.setBolt(boltId, new ReduceGoalStatements()).fieldsGrouping(prevId, new Fields("hashtag"));
		prevId = boltId;

		this.enableSQLOutput("sqloutput", prevId, builder);
		this.enableHDFSOutput("hdfsoutput", prevId, builder);
		//this.enablePrintOutput("printoutput", prevId, builder);

		StormTopology topology = builder.createTopology();
		return topology;
	}

	private void enablePrintOutput(String id, String sourceId,
			TopologyBuilder builder) {
		builder.setBolt(id, new PrinterBolt()).globalGrouping(sourceId);
	}

	private void enableHDFSOutput(String id, String sourceId,
			TopologyBuilder builder) {
		
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);
		
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(100,
				FileSizeRotationPolicy.Units.MB); // rotate files when they
													// reach 1KB
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(
				"/user/s1228838/output/").withExtension(".csv");
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter(",");

		HdfsBolt bolt = new HdfsBolt()
				.withFsUrl("hdfs://localhost:8020")
				.withFileNameFormat(fileNameFormat)
				.withRecordFormat(format)
				.withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy)
				.addRotationAction(
						new MoveFileAction().toDestination("/user/s1228838/old/"));

		builder.setBolt(id, bolt).shuffleGrouping(sourceId);
	}

	private void enableSQLOutput(String id, String sourceId,
			TopologyBuilder builder) {
		// All data should be directed to one bolt:
		// 1. It is only little data
		// 2. The server could probably not handle many simultaneously calling bolts.
		builder.setBolt(id, new SQLOutputBolt()).globalGrouping(sourceId);
	}

	private void setupKafkaSpout(String id, TopologyBuilder builder,
			Properties properties) {
		SpoutConfig spoutConf = new SpoutConfig(
				new ZkHosts(properties.getProperty("zkhost", "localhost:2181"),
						"/brokers"), properties.getProperty("topic",
						"worldcup"), "/kafka", "worldcup");

		spoutConf.forceFromStart = true;
		spoutConf.scheme = new TweetFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);

		builder.setSpout(id, spout);
	}

	public static void main(String[] args) {

		if (args.length < 2
				|| (!args[0].equals("local") && !args[0].equals("cluster"))) {
			System.out
					.println("Usage: storm <>.jar nl.utwente.bigdata.GoalDetector local|cluster session");
		} else {
			String[] a = new String[2];
			a[0] = "GoalDetector";
			a[1] = args[0];

			GoalDetector goalDetector = new GoalDetector();
			try{
				goalDetector.session = Integer.parseInt(args[1]);
			}catch(NumberFormatException e){
				System.out.println("Session should be an integer");
				return;
			}
			
			goalDetector.run(a);
		}
	}
}
