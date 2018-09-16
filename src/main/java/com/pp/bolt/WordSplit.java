package com.pp.bolt;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class WordSplit implements IRichBolt {
	
	private OutputCollector collector;

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	/**
     * *bolt*从单词文件接收到文本行，并标准化它。
     * 文本行会全部转化成小写，并切分它，从中得到所有单词。
    */
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		System.out.println(sentence);
		IKSegmenter ikSeg = new IKSegmenter(new StringReader(sentence), true); // 智能分词
        try {
            Lexeme lexeme = ikSeg.next();
            while(lexeme != null) {
            	collector.emit(new Values(lexeme.getLexemeText()));
            	lexeme = ikSeg.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //对元组做出应答
        collector.ack(input);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

	/**
     * 这个*bolt*只会发布“word”域
     */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
