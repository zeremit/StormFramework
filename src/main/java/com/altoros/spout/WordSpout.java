package com.altoros.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by dmitry.khorevich on 24.2.14.
 */
public class WordSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;

    Random random;

    String[] words = {"hello", "world", "from", "sun", "the", "cow", "jumped", "over", "the", "moon"};

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        random = new Random();
    }

    @Override
    public void nextTuple() {
        {
            String s = words[random.nextInt(words.length)];
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            collector.emit(new Values(s));
        }

    }
}
