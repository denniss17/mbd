package nl.utwente.bigdata;

import java.util.Properties;

import nl.utwente.bigdata.bolts.CheckGoalBolt;
import nl.utwente.bigdata.bolts.PrinterBolt;
import nl.utwente.bigdata.bolts.SQLOutputBolt;
import nl.utwente.bigdata.bolts.TweetJsonParseBolt;

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

public class GoalDetector extends AbstractTopologyRunner {
	
	@Override
	protected StormTopology buildTopology(Properties properties) {
		TopologyBuilder builder = new TopologyBuilder();

		String boltId = "";
		String prevId;

		// Set up the kafka spout
		boltId = "kafka";
		SpoutConfig spoutConf = new SpoutConfig(
				new ZkHosts(
						properties.getProperty("zkhost", "localhost:2181"), 
						"/brokers"),
				properties.getProperty("topic", "worldcup"), 
				"/kafka",
				"worldcup"
				);

		spoutConf.forceFromStart = true;
		spoutConf.scheme = new TweetFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);

		// Add the kafka spout
		builder.setSpout(boltId, spout);
		prevId = boltId;

		// Parse the tweet
		boltId = "parser";
		builder.setBolt(boltId, new TweetJsonParseBolt()).shuffleGrouping(prevId);
		prevId = boltId;

		// Extract goals
		// Output: 
		// time : Date the time of the tweet
		// country : String the name of the country which scored
		boltId = "checkgoal";
		builder.setBolt(boltId, new CheckGoalBolt()).shuffleGrouping(prevId);
		prevId = boltId;

		// Count the country occurrences
		/*boltId = "counter";
		builder.setBolt(boltId, new CountGoalBolt()).fieldsGrouping(prevId,
				new Fields("country"));
		prevId = boltId;*/

		//
		// boltId = "topcounter";
		// builder.setBolt(boltId, new
		// TopCounterBolt(25)).fieldsGrouping(prevId, new Fields("word")); //
		// "word" -> "word", "count"
		// prevId = boltId;
		
		// Create URLS
		boltId = "sqloutput";
		builder.setBolt(boltId, new SQLOutputBolt()).shuffleGrouping(prevId);
		prevId = boltId;

		/*
		 * OUTPUT 1: hdfs
		 */
		 boltId = "file";
		 SyncPolicy syncPolicy = new CountSyncPolicy(1000);
		 FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(100,
		 FileSizeRotationPolicy.Units.MB); // rotate files when they reach 1KB
		 FileNameFormat fileNameFormat = new
		 DefaultFileNameFormat().withPath("/output/").withExtension(".csv");
		 RecordFormat format = new
		 DelimitedRecordFormat().withFieldDelimiter(","); // use "|" instead of "," for field delimiter
		
		 HdfsBolt bolt = new HdfsBolt()
		 .withFsUrl("hdfs://localhost:8020")
		 .withFileNameFormat(fileNameFormat)
		 .withRecordFormat(format)
		 .withRotationPolicy(rotationPolicy)
		 .withSyncPolicy(syncPolicy)
		 .addRotationAction(new
		 MoveFileAction().toDestination("/output/old/"));
		
		 builder.setBolt(boltId, bolt).globalGrouping(prevId);

		/*
		 * OUTPUT 2: standard out
		 */
		boltId = "print";
		builder.setBolt(boltId, new PrinterBolt()).globalGrouping(prevId);

		StormTopology topology = builder.createTopology();
		return topology;
	}

	public static void main(String[] args) {
		if (args.length < 1 || (!args[0].equals("local") && !args[0].equals("cluster"))) {
			System.out.println("Usage: storm <>.jar nl.utwente.bigdata.GoalDetector local|cluster");
		}else{
			String[] a = new String[2];
			a[0] = "GoalDetector";
			a[1] = args[0];
			
			GoalDetector goalDetector = new GoalDetector();
			goalDetector.run(a);
		}

		
	}
}
