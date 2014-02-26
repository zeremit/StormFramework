package com.altoros.data;

import java.io.Serializable;

/**
 * Created by dmitry.khorevich on 13.2.14.
 */
public class Sensor implements Serializable {

    String id;

    int value;

    int tId;

    int max;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int gettId() {
        return tId;
    }

    public void settId(int tId) {
        this.tId = tId;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public Sensor(String id, int value, int tId) {
        this.id = id;
        this.value = value;
        this.tId = tId;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "id='" + id + '\'' +
                ", value=" + value +
                ", tId=" + tId +
                ", max=" + max +
                '}';
    }
}
