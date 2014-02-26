package com.altoros.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by dmitry.khorevich on 24.2.14.
 */
public class PrintWordCount extends BaseBasicBolt{
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");
        System.out.println("OUT>> [" + Thread.currentThread().getId() + "]" + word + " " + count );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
