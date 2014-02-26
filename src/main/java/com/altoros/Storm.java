package com.altoros;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.altoros.bolt.CountValuerBolt;
import com.altoros.bolt.MaxValueBolt;
import com.altoros.bolt.PrintOutBolt;
import com.altoros.data.Sensor;
import com.altoros.spout.SensorGrouper;
import com.altoros.spout.SensorSpout;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class Storm {

    public static void main( String[] args ) throws InterruptedException {
        System.out.println("storm start");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SensorSpout.class.toString(), new SensorSpout());
//        builder.setBolt(CountValuerBolt.class.toString(), new CountValuerBolt(), SensorSpout.TID).fieldsGrouping(SensorSpout.class.toString(), new Fields("tID"));
        builder.setBolt(CountValuerBolt.class.toString(), new CountValuerBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(MaxValueBolt.class.toString(), new MaxValueBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(PrintOutBolt.class.toString(), new PrintOutBolt(), 2).shuffleGrouping(MaxValueBolt.class.toString());

        Config config = new Config();
        config.setDebug(true);
       // config.setNumWorkers(4);
        //config.setNumWorkers(32);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T1", config, builder.createTopology());
        Thread.sleep(1000 * 10);
        cluster.shutdown();


    }
}
