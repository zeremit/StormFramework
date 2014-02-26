package com.altoros.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.altoros.data.Sensor;

import java.util.Collections;
import java.util.LinkedList;

/**
 * Created by dmitry.khorevich on 14.2.14.
 */
public class MaxValueBolt extends BaseBasicBolt{

    private LinkedList<Integer> elem = new LinkedList<Integer>();

    private static final int N = 10;

    OutputCollector outputCollector;

    int max = 0;

    int count = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Sensor sensor = (Sensor) tuple.getValue(0);
        int sensorValue = sensor.getValue();
        elem.add(sensorValue);
        if(count>=N){
            elem.removeFirst();
        }else {
            count++;
        }
        sensor.setMax(Collections.max(elem));
        System.out.println("OUT>> [" + Thread.currentThread().getId() + "] tId=" + sensor.gettId() + " max=" + max);
        basicOutputCollector.emit(new Values(tuple));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("max"));
    }
}
