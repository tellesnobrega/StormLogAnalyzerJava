package openstack.summit;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import openstack.summit.ReadLogLines;
import openstack.summit.bolt.AlarmBolt;
import openstack.summit.bolt.FilterErrorBolt;

import org.apache.kafka.clients.producer.ProducerConfig;

import storm.kafka.Broker;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


public class StormLogAnalyzerTopologyLocal {
	
	public static void main(String[] args) {
		final int numSpouts = 1;
		final Number numBolts = 1;
		final String hostBroker = "localhost";
		final int brokerPort = 9092;
		final String topic = "logs";
		final int tempoExecucao = 300;
		final String filePath = "/home/tellesmvn/workspace/KafkaProducer/sahara-all-small.log";
		
	    TopologyBuilder topologyBuilder = new TopologyBuilder();

	    GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
	    
		for (int i = 0; i < numSpouts; i++) {
		    globalPartitionInformation.addPartition(i, new Broker(hostBroker, brokerPort));
	    }

	    StaticHosts staticHosts = new StaticHosts(globalPartitionInformation);
	    
		SpoutConfig spoutConfig = new SpoutConfig(staticHosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), numSpouts);
		topologyBuilder.setBolt("filterErrorBolt", new FilterErrorBolt(), 4).shuffleGrouping("spout");
		topologyBuilder.setBolt("alarmBolt", new AlarmBolt(hostBroker), 12).fieldsGrouping("filterErrorBolt", new Fields("component"));
		

		Config config = new Config();
		config.setNumWorkers(3);
//		config.setDebug(true);
		config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		
		Thread thread = new Thread() {
	    	public void run() {
			    Map<String, Object> props = new HashMap<>();
			    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostBroker + ":" + brokerPort);
			    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			    props.put(ProducerConfig.CLIENT_ID_CONFIG, "log-producer");
			    
			    Utils.sleep(30000);
				
			    new ReadLogLines(filePath).getLine(props, "logs");
	    	}
	    };
	    
	    thread.setDaemon(true);
		thread.start();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-log-analyzer", config, topologyBuilder.createTopology());
		Utils.sleep(tempoExecucao * 1000);
//		cluster.killTopology("calculo-media-movel");
		cluster.shutdown();
	}

}
