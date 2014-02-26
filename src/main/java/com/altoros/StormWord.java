package com.altoros;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.altoros.bolt.CountValuerBolt;
import com.altoros.bolt.PrintWordCount;
import com.altoros.bolt.WordCountBolt;
import com.altoros.spout.SensorSpout;
import com.altoros.spout.WordSpout;

/**
 * Created by dmitry.khorevich on 24.2.14.
 */
public class StormWord {

    public static void main( String[] args ) throws InterruptedException {
        System.out.println("storm start");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spot", new WordSpout());
        builder.setBolt(WordCountBolt.class.toString(), new WordCountBolt(), 2).fieldsGrouping("spot", new Fields("word"));
        builder.setBolt(PrintWordCount.class.toString(), new PrintWordCount(), 2).shuffleGrouping(WordCountBolt.class.toString());
        //  builder.setBolt(CountValuerBolt.class.toString(), new CountValuerBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(MaxValueBolt.class.toString(), new MaxValueBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(PrintOutBolt.class.toString(), new PrintOutBolt(), 2).shuffleGrouping(MaxValueBolt.class.toString());

        Config config = new Config();
        config.setDebug(false);
        // config.setNumWorkers(4);
        //config.setNumWorkers(32);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T1", config, builder.createTopology());
        Thread.sleep(1000 * 10);
        cluster.shutdown();


    }
}
