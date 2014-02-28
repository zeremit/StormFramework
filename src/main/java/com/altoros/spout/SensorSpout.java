package com.altoros.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.altoros.data.Sensor;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class SensorSpout extends BaseRichSpout {

    private int count;
    private SpoutOutputCollector collector;

    Random random;

    private final static int NUMBER_OF_DATA = 100;

    private final static int VALUE = 100;

    public final static int TID = 10;

    private Sensor getSensor(){
        return new Sensor(UUID.randomUUID().toString(), random.nextInt(VALUE),random.nextInt(TID));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensor"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        random = new Random();
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(10);
          //  if(++count > NUMBER_OF_DATA) { return; }
        } catch (InterruptedException e) {}
        Sensor sensor = getSensor();
        collector.emit(new Values(sensor));
    }
}
