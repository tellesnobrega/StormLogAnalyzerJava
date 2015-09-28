package openstack.summit;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import openstack.summit.bolt.FilterErrorBolt;

import org.apache.kafka.clients.producer.ProducerConfig;

import storm.kafka.Broker;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class StormLogAnalyzerTopology {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Please Inform: <host-broker> <topic>");
			System.exit(-1);
		}

		final String hostBroker = args[0];
		final String topic = args[1];
		final int NUM_SPOUTS = 1;
		final int BROKER_PORT = 9092;
		
		
		Map<?, ?> clusterConf = Utils.readStormConfig();
		clusterConf.putAll(Utils.readCommandLineOpts());
		
		try {
		    
		    TopologyBuilder topologyBuilder = new TopologyBuilder();
		    
		    GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
		    
			for (int i = 0; i < NUM_SPOUTS; i++) {
			    globalPartitionInformation.addPartition(i, new Broker(hostBroker, BROKER_PORT));
		    }
			
		    StaticHosts staticHosts = new StaticHosts(globalPartitionInformation);

			SpoutConfig spoutConfig = new SpoutConfig(staticHosts, topic, "/" + topic, UUID.randomUUID().toString());

			topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
			topologyBuilder.setBolt("bolt", new FilterErrorBolt(), 1).shuffleGrouping("spout");
	
			Config config = new Config();
			config.setNumWorkers(3);
			config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
			config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
			config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
			config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

			config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

			StormSubmitter.submitTopologyWithProgressBar("calculo-media-movel", config, topologyBuilder.createTopology());
			
		    Thread thread = new Thread() {
		    	public void run() {
				    Map<String, Object> props = new HashMap<>();
				    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostBroker + ":" + 9092);
				    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
				    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
				    props.put(ProducerConfig.CLIENT_ID_CONFIG, "log-line");
				    
				    new ReadLogLines().getLine(props, topic);
		    	}
		    };
		    
		    thread.setDaemon(true);
			thread.start();
		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
//			KillOptions killOpts = new KillOptions();
//			killOpts.set_wait_secs(0);
//			client.killTopologyWithOpts("calculo-media-movel", killOpts);
			
//			System.exit(0);
		}

	}
}
