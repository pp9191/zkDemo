package com.pp.zk;

import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class FirstStorm {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RandomSpout());
		builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(false);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("firststorm", conf, builder.createTopology());
			Utils.sleep(30000);
			cluster.killTopology("firststorm");
			cluster.shutdown();
		}
	}

}

class SenqueceBolt extends BaseBasicBolt {

	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		String word = (String) arg0.getValue(0);
		String out = "Hello " + word + "!";
		System.out.println(out);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}

class RandomSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private boolean isCompleted = false;
	private static String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};

	public void nextTuple() {
		String word = words[new Random().nextInt(words.length)];
		collector.emit(new Values(word));
//		if (isCompleted) {
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// 什么也不做
//			}
//			return;
//		}
//		for (int i = 0; i < words.length; i++) {			
//			String word = words[new Random().nextInt(words.length)];
//			collector.emit(new Values(word));			
//		}
//		isCompleted = true;
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		this.collector = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("randomstring"));
	}

}
