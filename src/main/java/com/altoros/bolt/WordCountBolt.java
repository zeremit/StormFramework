package com.altoros.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dmitry.khorevich on 24.2.14.
 */
public class WordCountBolt extends BaseBasicBolt{

    Map<String, Integer> map = new HashMap<String,Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String s = tuple.getStringByField("word");
        Integer i = map.get(s);
        if(i==null){
            i=0;
        }
        i++;
        map.put(s, i);
        basicOutputCollector.emit(new Values(s,i));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
           outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
