package com.pp.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.pp.bolt.WordCounter;
import com.pp.bolt.WordSplit;
import com.pp.spout.WordReader;

public class TopologyMain {

	public static void main(String[] args) {
		//定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordSplit()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word"));

        //配置
        Config conf = new Config();
        //System.out.println("filePath:" + args[0]);
        conf.put("wordsFile", "src/main/resources/word.txt");
        conf.setDebug(false);

        //运行拓扑
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();        
        cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
        try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Utils.sleep(30000);
        cluster.shutdown();
	}

}
