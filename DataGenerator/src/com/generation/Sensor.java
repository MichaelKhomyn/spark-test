package com.generation;

import java.util.UUID;

public class Sensor {

    Sensor(Boolean present, double temperature, long timeCounter){
        setBattery(100.0);
        setPresent(present);
        setTemperature(temperature);
        setSensorId(UUID.randomUUID());
        setTimeCounter(timeCounter);
        setSensorChannel(1);
    }

    public double getBattery() {
        return battery;
    }

    public void setBattery(double battery) {
        this.battery = battery;
    }

    public Boolean getPresent() {
        return present;
    }

    public void setPresent(Boolean present) {
        this.present = present;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public UUID getSensorId() {
        return sensorId;
    }

    private void setSensorId(UUID sensorId) {
        this.sensorId = sensorId;
    }

    public long getTimeCounter() {
        return timeCounter;
    }

    public void setTimeCounter(long timeCounter) {
        this.timeCounter = timeCounter;
    }

    public long getSensorChannel() {
        return sensorChannel;
    }

    public void setSensorChannel(long sensorChannel) {
        this.sensorChannel = sensorChannel;
    }

    private UUID sensorId;
    private double battery;
    private Boolean present;
    private double temperature;
    private long timeCounter;
    private long sensorChannel;
}
