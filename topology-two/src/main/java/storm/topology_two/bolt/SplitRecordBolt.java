package storm.topology_two.bolt;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Bolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitRecordBolt extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3031084336218389707L;
	private final static Logger LOGGER = LoggerFactory.getLogger(SplitRecordBolt.class);
	private OutputCollector collector;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String type = input.getStringByField("type");
		String record = input.getStringByField("record");
		for (String word : record.split("\\s+")) {
			collector.emit(input, new Values(type, word));
			LOGGER.info("Word emmited: type = " + type + ", word = " + word);
			collector.ack(input);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("type", "word"));
	}
}
