package storm.topology_two.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.topology_two.bolt.DistributeWordByTypeBolt;
import storm.topology_two.bolt.SaveDataBolt;
import storm.topology_two.bolt.SplitRecordBolt;
import storm.topology_two.spout.ProduceRecordSpout;
import storm.topology_two.type.Type;

public class Topology {

	public static String[] numberRecords = new String[]{
			"11 11",
			"22 2 2",
			"3 3 3 3 9",
			"-2 21 2",
			"123 31 23 09"
	};
	
	public static String[] stringRecords = new String[]{
			"aa abc dd ll",
			"abc bb ii",
			"hello kk",
			"alibaba tmall"
	};
	
	public static String[] signRecords = new String[]{
			"++ -- /s",
			"- * )(",
			". > <"
	};
	
	public static void main(String[] args) throws Exception{
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("spout-number", new ProduceRecordSpout(Type.NUMBER, numberRecords), 1);
		topologyBuilder.setSpout("spout-string", new ProduceRecordSpout(Type.STRING, stringRecords), 1);
		topologyBuilder.setSpout("spout-sign", new ProduceRecordSpout(Type.SIGN, signRecords), 1);

		BoltDeclarer numberSaveBolt = topologyBuilder.setBolt("bolt-number", new SaveDataBolt(Type.NUMBER), 3);
		BoltDeclarer stringSaveBolt = topologyBuilder.setBolt("bolt-string", new SaveDataBolt(Type.STRING), 3);
		BoltDeclarer signSaveBolt = topologyBuilder.setBolt("bolt-sign", new SaveDataBolt(Type.SIGN), 3);
		BoltDeclarer distributeBolt = topologyBuilder.setBolt("bolt-distribute", new DistributeWordByTypeBolt(), 6);
		BoltDeclarer splitBolt = topologyBuilder.setBolt("bolt-split", new SplitRecordBolt(), 3);

		splitBolt
		.shuffleGrouping("spout-string")
		.shuffleGrouping("spout-number")
		.shuffleGrouping("spout-sign");

		distributeBolt.fieldsGrouping("bolt-split", new Fields("type"));
		
		numberSaveBolt.localOrShuffleGrouping("bolt-distribute", "number-count-stream");
		stringSaveBolt.localOrShuffleGrouping("bolt-distribute", "string-count-stream");
		signSaveBolt.localOrShuffleGrouping("bolt-distribute", "sign-count-stream");

		Config conf = new Config();
	    String name = Topology.class.getSimpleName();
	    
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology(name, conf, topologyBuilder.createTopology());
	    
	}
}
