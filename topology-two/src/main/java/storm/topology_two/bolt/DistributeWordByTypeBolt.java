package storm.topology_two.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.topology_two.type.Type;

public class DistributeWordByTypeBolt extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5755269530239086076L;
	private static final Logger LOGGER = LoggerFactory.getLogger(DistributeWordByTypeBolt.class);
	private OutputCollector collector;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String type = input.getStringByField("type");
		String word = input.getStringByField("word");
		LOGGER.info("Word Distribute: type = " + type + ", word = " + word + ", count = 1" );
		if (Type.STRING.get().equals(type)) {
			collector.emit("string-count-stream", input, new Values(type, word, 1));
		}else if (Type.NUMBER.get().equals(type)) {
			collector.emit("number-count-stream", input, new Values(type, word, 1));
		}else if (Type.SIGN.get().equals(type)) {
			collector.emit("sign-count-stream", input, new Values(type, word, 1));
		}

		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("string-count-stream", new Fields("type", "word", "count"));
		declarer.declareStream("number-count-stream", new Fields("type", "word", "count"));
		declarer.declareStream("sign-count-stream", new Fields("type", "word", "count"));
	}

}
