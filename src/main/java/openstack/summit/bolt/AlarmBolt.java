package openstack.summit.bolt;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class AlarmBolt extends BaseRichBolt {

	private static final long serialVersionUID = -3752711690604033901L;
	private OutputCollector collector;
	private Date baseTimestamp;
	private Date currentTimestamp;
	private int counter;

	public AlarmBolt() {
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.baseTimestamp = null;
		this.currentTimestamp = null;
		this.counter = 0;
	}
	
	public void execute(Tuple input) {
		String timestamp = input.getString(0);
		try {
			Date newTimestamp = this.convertToDate(timestamp);
			int errors = getNumErrors(newTimestamp);
			if(errors == 3) {
				Map<String, Object> props = new HashMap<>();
			    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			    props.put(ProducerConfig.CLIENT_ID_CONFIG, "storm-output");
			    
			    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			    	producer.send(new ProducerRecord<String, String>("alarm", "There were 3 errors in the last 3 minutes"));
			    }
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		collector.ack(input);
	}
	
	private Date convertToDate(String timestamp) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		Date parsedDate = dateFormat.parse(timestamp);
		return parsedDate;
	}
	
	private long timeDiff(Date newTimestamp, Date currentTimestamp) {
		long diff = newTimestamp.getTime() - currentTimestamp.getTime();
		return diff;
	}
	
	private int getNumErrors(Date newTimestamp) {
		if (this.baseTimestamp != null) {
            if (this.timeDiff(newTimestamp, this.baseTimestamp) <= MILLISECONDS.convert(3, MINUTES)) {
                this.currentTimestamp = newTimestamp;
                this.counter += 1;
            } else {
                if (this.currentTimestamp != null) {
                    if (this.timeDiff(newTimestamp, this.currentTimestamp) <= MILLISECONDS.convert(3, MINUTES)) {
                        this.baseTimestamp = this.currentTimestamp;
                        this.currentTimestamp = newTimestamp;
                        this.counter = 2;
                    } else {
                        this.baseTimestamp = newTimestamp;
                        this.counter = 1;
                    }
                } else {
                    this.baseTimestamp = newTimestamp;
                    this.counter = 1;
                }
            }
        } else {
            this.baseTimestamp = newTimestamp;
            this.counter = 1;
        }
		return counter;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
}
