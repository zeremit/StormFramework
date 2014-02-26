package com.altoros.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.altoros.data.Sensor;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class CountValuerBolt extends BaseBasicBolt {

    OutputCollector outputCollector;

    private static final int N = 10;

    int value = 0;

    int count = 0;

    LinkedList<Integer> elem = new LinkedList<Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Sensor sensor = (Sensor) tuple.getValue(0);
        int sensorValue = sensor.getValue();
        elem.add(sensorValue);
        value+= sensorValue;
        if(count>=N){
            int t = elem.getFirst();
            elem.removeFirst();
            value-= t;
        }else{
            count++;
        }
        System.out.println("OUT>> [" + Thread.currentThread().getId() + "] tId=" + sensor.gettId()+ " sum.=" + value + " avg.=" + ((double)value)/count);
      //  outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
