package com.altoros.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.altoros.data.Sensor;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class PrintOutBolt extends BaseBasicBolt{

    OutputCollector outputCollector;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Object t = tuple.getValue(0);
        System.out.println("OUT>> [" + Thread.currentThread().getId() + "]" + t.toString());
      //  outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }
}
