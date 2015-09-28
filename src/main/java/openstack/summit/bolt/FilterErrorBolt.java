package openstack.summit.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterErrorBolt extends BaseRichBolt {

	private static final long serialVersionUID = -3752711690604033901L;
	private OutputCollector collector;

	public FilterErrorBolt() {
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String logLine = input.getString(0);
		if (logLine.contains("ERROR")) {
			String timestamp = getTimestamp(logLine);
			collector.emit(new Values(timestamp));
		}
		collector.ack(input);
	}
	
	private String getTimestamp(String logLine) {
		String[] splittedString = logLine.split(" ");
		String timestamp = splittedString[0] + " " + splittedString[1];
		return timestamp;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("alarm"));
	}
	
}
