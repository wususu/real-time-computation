package storm.topology_two.bolt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.monitor.CounterMonitor;

import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import storm.topology_two.CounterMapUtils;
import storm.topology_two.type.Type;

public class SaveDataBolt extends BaseRichBolt{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1447670521890169514L;
	private final static Logger LOGGER = LoggerFactory.getLogger(SaveDataBolt.class);
	private OutputCollector collector;
	private Map<String, AtomicLong> map;
	private Type TYPE;
	
	public SaveDataBolt(Type TYPE) {
		// TODO Auto-generated constructor stub
		this.TYPE = TYPE;
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String word = input.getStringByField("word");
		Integer count = input.getIntegerByField("count");

		switch (TYPE) {
		case NUMBER:
			this.map = CounterMapUtils.numberCounter;
			break;
		case SIGN:
			this.map = CounterMapUtils.signCounter;
			break;
		case STRING:
			this.map = CounterMapUtils.stringCounter;
			break;
		default:
			break;
		}
		AtomicLong al= this.map.get(word);
		if (al == null) {
			al = new AtomicLong(0);
			this.map.put(word, al);
		}
		al.addAndGet((long)count);
		LOGGER.info("word save: type = " + TYPE.get() + ", word = " + word + this.map.toString());
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}


	
}
