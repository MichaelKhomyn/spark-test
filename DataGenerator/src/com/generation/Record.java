package com.generation;
import java.util.UUID;

public class Record {

    public Record(int sensorChannel, String channelType, String value, Long timestamp){
        this.sensorChannel = sensorChannel;
        this.channelType = channelType;
        this.value = value;
        this.timestamp = timestamp;
    }

    public int getSensorChannel() {
        return sensorChannel;
    }

    public String getChannelType() {
        return channelType;
    }

    public String getValue() {
        return value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    private int sensorChannel;
    private String channelType;
    private String value;
    private Long timestamp;
}
