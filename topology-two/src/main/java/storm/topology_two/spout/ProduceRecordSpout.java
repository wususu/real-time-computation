package storm.topology_two.spout;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.topology_two.type.Type;

public class ProduceRecordSpout extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2520720012605705022L;

	private final Type TYPE;
	
	private SpoutOutputCollector collector;
	
	private Random random;
	
	private String[] recordLines;
	
	public static final Logger LOGGER = LoggerFactory.getLogger(ProduceRecordSpout.class);
	
	public ProduceRecordSpout(Type TYPE, String[] recordLines) {
		// TODO Auto-generated constructor stub
		this.TYPE = TYPE;
		this.recordLines = recordLines;
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.random = new Random();
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(2000);
		String record = recordLines[random.nextInt(recordLines.length)];
		List<Object> values = new Values(TYPE.get(), record);
		LOGGER.info("Record emmited: type = " + TYPE.get() + ", record = " + record);

		collector.emit(values, values);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("type", "record"));
	}
}
