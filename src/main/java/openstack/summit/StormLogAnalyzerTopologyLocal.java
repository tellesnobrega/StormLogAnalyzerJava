package openstack.summit;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import openstack.summit.bolt.AlarmBolt;
import openstack.summit.bolt.FilterErrorBolt;

import org.apache.kafka.clients.producer.ProducerConfig;

import storm.kafka.Broker;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


public class StormLogAnalyzerTopologyLocal {
	
	public static void main(String[] args) {
		final int numSpouts = 1;
		final Number numBolts = 1;
		final String hostBroker = "localhost";
		final int brokerPort = 9092;
		final String topic = "logs";
		final int tempoExecucao = 300;
		
	    TopologyBuilder topologyBuilder = new TopologyBuilder();

	    GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
	    
		for (int i = 0; i < numSpouts; i++) {
		    globalPartitionInformation.addPartition(i, new Broker(hostBroker, brokerPort));
	    }

	    StaticHosts staticHosts = new StaticHosts(globalPartitionInformation);
	    
		SpoutConfig spoutConfig = new SpoutConfig(staticHosts, topic, "/" + topic, UUID.randomUUID().toString());

		topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), numSpouts);
		topologyBuilder.setBolt("filterErrorBolt", new FilterErrorBolt(), numBolts).shuffleGrouping("spout");
		topologyBuilder.setBolt("alarmBolt", new AlarmBolt(), numBolts).shuffleGrouping("filterErrorBolt");
		

		Config config = new Config();
		config.setNumWorkers(3);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		
		config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

		Thread thread = new Thread() {
	    	public void run() {
			    Map<String, Object> props = new HashMap<>();
			    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostBroker + ":" + brokerPort);
			    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			    props.put(ProducerConfig.CLIENT_ID_CONFIG, "media-producer");
			    
			    Utils.sleep(30000);
				// garantir que dados ainda sejam gerados caso demore um pouco para a topologia iniciar.
			    // a thread eh daemon, entao nao vai impedir a finalizacao.
//		    	new GeradorMedicaoAleatoria(taxaEntrada, percentualSleep, criptografado).gerar(props, topic, tempoExecucao * 2);
	    	}
	    };
	    
	    thread.setDaemon(true);
		thread.start();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("calculo-media-movel", config, topologyBuilder.createTopology());
		Utils.sleep(tempoExecucao * 1000);
		cluster.killTopology("calculo-media-movel");
		cluster.shutdown();
	}

}
