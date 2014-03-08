package com.altoros;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.altoros.bolt.CountValueTestBolt;
import com.altoros.spout.SensorGrouper;
import com.altoros.spout.SensorSpout;
import org.apache.commons.cli.*;

import java.util.UUID;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class Storm {

    static boolean runOnCluster = true;

    static private int worker = 1;

    static private boolean debug = false;

    static private String name = "T1";

    public static void main( String[] args ) throws InterruptedException, ParseException {

        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("worker").hasArgs(1).isRequired(false).create());
        options.addOption(OptionBuilder.withLongOpt("name").hasArgs(1).isRequired(false).create());
        options.addOption(OptionBuilder.withLongOpt("cluster").hasArgs(1).isRequired(false).create());
        options.addOption(OptionBuilder.withLongOpt("debug").hasArgs(1).isRequired(false).create());
        CommandLineParser cmdLinePosixParser = new PosixParser();// создаем Posix парсер
        CommandLine commandLine = cmdLinePosixParser.parse(options, args);
        if (commandLine.hasOption("worker")) { // проверяем, передавали ли нам команду l, сравнение будет идти с первым представлением опции, в нашем случаее это было однобуквенное представление l
            String arguments = commandLine.getOptionValue("worker", "1");// если такая опция есть, то получаем переданные ей аргументы
            System.out.println("Worker: " + arguments);// выводим переданный логин
        }
        if (commandLine.hasOption("name")) { // проверяем, передавали ли нам команду l, сравнение будет идти с первым представлением опции, в нашем случаее это было однобуквенное представление l
            String arguments = commandLine.getOptionValue("name","T" + UUID.randomUUID());// если такая опция есть, то получаем переданные ей аргументы
            System.out.println("Topology name: " + arguments);// выводим переданный логин
        }
        if (commandLine.hasOption("local")) { // проверяем, передавали ли нам команду l, сравнение будет идти с первым представлением опции, в нашем случаее это было однобуквенное представление l
            String arguments = commandLine.getOptionValue("local",Boolean.TRUE.toString());// если такая опция есть, то получаем переданные ей аргументы
            runOnCluster = Boolean.parseBoolean(arguments);
            System.out.println("Run On Cluster: " + runOnCluster);// выводим переданный логин
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SensorSpout.class.toString(), new SensorSpout());
//        builder.setBolt(CountValuerBolt.class.toString(), new CountValuerBolt(), SensorSpout.TID).fieldsGrouping(SensorSpout.class.toString(), new Fields("tID"));
        builder.setBolt(CountValueTestBolt.class.toString(), new CountValueTestBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(MaxValueBolt.class.toString(), new MaxValueBolt(), SensorSpout.TID).customGrouping(SensorSpout.class.toString(), new SensorGrouper());
//        builder.setBolt(PrintOutBolt.class.toString(), new PrintOutBolt(), 2).shuffleGrouping(MaxValueBolt.class.toString());

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
        //config.setNumWorkers(32);

        if(!runOnCluster){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("T1", config, builder.createTopology());
            try { Thread.sleep(10000); } catch (InterruptedException ex) {ex.printStackTrace();}
            cluster.shutdown();
       // Thread.sleep(1000 * 10);
        //cluster.shutdown();
        }else{
            try {
                StormSubmitter.submitTopology("T1", config, builder.createTopology());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }


    }
}
