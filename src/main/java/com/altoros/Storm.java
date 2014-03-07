package com.altoros;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.daemon.worker;
import backtype.storm.topology.TopologyBuilder;
import com.altoros.bolt.CountValueTestBolt;
import com.altoros.spout.SensorGrouper;
import com.altoros.spout.SensorSpout;
import org.apache.commons.cli.*;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class Storm {

    static boolean runLocal = true;

    public static void main( String[] args ) throws InterruptedException, ParseException {

        Options options = new Options();
        Option option = new Option("worker", false, "Number of worker");
        option.setRequired(false);
        options.addOption(option);
        Option optionName = new Option("name", false, "Topology name");
        options.addOption(optionName);
        CommandLineParser cmdLinePosixParser = new PosixParser();// создаем Posix парсер
        CommandLine commandLine = cmdLinePosixParser.parse(options, args);
        if (commandLine.hasOption("worker")) { // проверяем, передавали ли нам команду l, сравнение будет идти с первым представлением опции, в нашем случаее это было однобуквенное представление l
            String[] arguments = commandLine.getOptionValues("worker");// если такая опция есть, то получаем переданные ей аргументы
            System.out.println("Worker: " + arguments[0]);// выводим переданный логин
        }
        if (commandLine.hasOption("name")) { // проверяем, передавали ли нам команду l, сравнение будет идти с первым представлением опции, в нашем случаее это было однобуквенное представление l
            String[] arguments = commandLine.getOptionValues("name");// если такая опция есть, то получаем переданные ей аргументы
            System.out.println("Topology name: " + arguments[0]);// выводим переданный логин
        }

        System.out.println("storm start");
        System.out.println("\tName:" + "runLocal" + ", Value:" + System.getProperty("runLocal"));
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

        if(runLocal){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("T1", config, builder.createTopology());
            try { Thread.sleep(10000); } catch (InterruptedException ex) {}
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
