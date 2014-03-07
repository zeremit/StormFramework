package com.altoros;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.altoros.bolt.PrintWordCount;
import com.altoros.bolt.WordCountBolt;
import com.altoros.spout.WordSpout;

/**
 * Created by dmitry.khorevich on 24.2.14.
 */
public class StormWord {

    static boolean runLocal = false;

    public static void main( String[] args ) throws InterruptedException {
        System.out.println("storm start");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spot", new WordSpout(), 4);
        builder.setBolt(WordCountBolt.class.toString(), new WordCountBolt(), 2).fieldsGrouping("spot", new Fields("word"));
        builder.setBolt(PrintWordCount.class.toString(), new PrintWordCount(), 2).shuffleGrouping(WordCountBolt.class.toString());
        //  builder.setBolt(CountValuerBolt.class.toString(), new CountValuerBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(MaxValueBolt.class.toString(), new MaxValueBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(PrintOutBolt.class.toString(), new PrintOutBolt(), 2).shuffleGrouping(MaxValueBolt.class.toString());

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(6);
        //config.setNumWorkers(32);

        if(runLocal){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("T1", config, builder.createTopology());
            try { Thread.sleep(10000); } catch (InterruptedException ex) {}
            cluster.shutdown();
            // Thread.sleep(1000 * 10);
            //cluster.shutdown();
        }else{
            try {
                StormSubmitter.submitTopology("T2", config, builder.createTopology());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }
}
