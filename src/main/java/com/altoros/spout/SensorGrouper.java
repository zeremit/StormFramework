package com.altoros.spout;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.altoros.data.Sensor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class SensorGrouper implements CustomStreamGrouping {
    private List<Integer> tasks;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId,
                        List<Integer> integers) {
        tasks = new ArrayList<>(integers);
        System.err.println(tasks.size());
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> objects) {
        List<Integer> rvalue = new ArrayList<>(objects.size());
        for(Object o: objects) {
            Sensor sensor = (Sensor) o;
            System.out.println("OUT>> [" + Thread.currentThread().getId() + "] grooping" );
            rvalue.add(tasks.get(sensor.gettId()));
        }

        return rvalue;
    }
}
