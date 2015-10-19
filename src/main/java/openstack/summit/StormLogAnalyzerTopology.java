package openstack.summit;

import java.util.Map;
import java.util.UUID;

import openstack.summit.bolt.AlarmBolt;
import openstack.summit.bolt.FilterErrorBolt;
import storm.kafka.Broker;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class StormLogAnalyzerTopology {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Please Inform: <topology-name> <host-broker> <topic>");
			System.exit(-1);
		}
		final String topologyName = args[0];
		final String hostBroker = args[1];
		final String topic = args[2];
		final int NUM_SPOUTS = 1;
		final int BROKER_PORT = 9092;
		
		Map<?, ?> clusterConf = Utils.readStormConfig();
		clusterConf.putAll(Utils.readCommandLineOpts());
		
		try {

			TopologyBuilder topologyBuilder = new TopologyBuilder();

			GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();

			for (int i = 0; i < NUM_SPOUTS; i++) {
				globalPartitionInformation.addPartition(i, new Broker(
						hostBroker, BROKER_PORT));
			}

			StaticHosts staticHosts = new StaticHosts(
					globalPartitionInformation);

			SpoutConfig spoutConfig = new SpoutConfig(staticHosts, topic, "/"
					+ topic, UUID.randomUUID().toString());
			spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

			topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
			topologyBuilder
					.setBolt("filterErrorBolt", new FilterErrorBolt(), 4)
					.shuffleGrouping("spout");
			topologyBuilder.setBolt("alarmBolt", new AlarmBolt(hostBroker), 12)
					.fieldsGrouping("filterErrorBolt", new Fields("component"));

			Config config = new Config();
			config.setNumWorkers(3);
			config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
			config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
			config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
			config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

			StormSubmitter.submitTopologyWithProgressBar(topologyName, config,
					topologyBuilder.createTopology());

		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
}
