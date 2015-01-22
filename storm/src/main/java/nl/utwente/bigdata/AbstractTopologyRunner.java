package nl.utwente.bigdata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

public abstract class AbstractTopologyRunner {

	// sub-classes have to override this function
	protected abstract StormTopology buildTopology(Properties properties);

	public void runLocal(String name, Properties properties) {
		StormTopology topology = buildTopology(properties);
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(name, conf, topology);
	}

	// start
	public void runCluster(String name, Properties properties) throws AlreadyAliveException, InvalidTopologyException {
		StormTopology topology = buildTopology(properties);
		Config conf = new Config();
		conf.put(Config.NIMBUS_HOST, properties.getProperty("nimbus", "ctithead1.ewi.utwente.nl"));
		System.out.println(conf.toString());
		StormSubmitter.submitTopology(name, conf, topology);
	}

	// Starts a topology based on it's command line
	public void run(String[] args) {
		try {
			Properties properties = new Properties();
			if (args.length > 0) {
				properties.load(new FileInputStream(args[0]));
			}
			String name = properties.getProperty("name", "GoalDetector");
			String type = properties.getProperty("type", "");
			if (type.equals("local")) {
				runLocal(name, properties);
			} else if (type.equals("cluster")) {
				runCluster(name, properties);
			} else{
				System.out.println("Type should be 'local' or 'cluster'");
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	}

}
