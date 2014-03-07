package com.altoros.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.altoros.data.Sensor;

import java.util.LinkedList;

/**
 * Created by dmitry.khorevich on 6.3.14.
 */
public class CountValueTestBolt extends BaseBasicBolt{

    OutputCollector outputCollector;

    private static final int N = 10;

    int value = 0;

    int count = 0;

    LinkedList<Integer> elem = new LinkedList<Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Sensor sensor = (Sensor) tuple.getValue(0);
        count++;
        System.out.println("OUT>> [" + Thread.currentThread().getId() + "] tId=" + sensor.gettId()+ " count=" + count);
        //  outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
