package com.generation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGen {

    private final static int ROOM_MAX_SIZE = 4;
    private final static int NUMBER_OF_ROOMS = 4;
    private final static long START_DATE = System.currentTimeMillis();
    private final static long END_DATE = START_DATE + 3*60*60*1000;
    private final static int PRESENCE_FREQUENCY_SEC = 120;
    private final static int BATTERY_FREQUENCY_SEC = 120;
    private final static int TEMPERATURE_FREQUENCY_SEC = 5;
    private final static double ROOM_TEMPERATURE = 65.0;
    private final static int BATTERY_LIVE_TIME_SEC = 36000;
    private final static String OUTPUT_FOLDER = "/home/mike/gen_data/";

    public static void main(String[] args){

        List<Room> roomsList = new ArrayList<>();

        Random rand = new Random();
        for(int i = 0; i < NUMBER_OF_ROOMS; i++){
            double temperature = ROOM_TEMPERATURE + (-0.2 + 0.4 * rand.nextDouble());
            roomsList.add(new Room(ROOM_MAX_SIZE, temperature, PRESENCE_FREQUENCY_SEC));
        }

        Long currentTime = START_DATE;
        Double batteryExhaustion = 100.0 / BATTERY_LIVE_TIME_SEC;

        BufferedWriter bwDataSource1;
        BufferedWriter bwDataSource2;


        try {
            new File(OUTPUT_FOLDER + "ds1.csv").delete();
            new File(OUTPUT_FOLDER + "ds2.csv").delete();

            bwDataSource1 = new BufferedWriter(new FileWriter(OUTPUT_FOLDER + "ds1.csv", true));
            bwDataSource2 = new BufferedWriter(new FileWriter(OUTPUT_FOLDER + "ds2.csv", true));

            while(currentTime < END_DATE){

                for(Room room: roomsList){
                    for(Sensor sensor: room.getRoomSensors()){
                        if (sensor.getBattery() > 0){
                            if(sensor.getTimeCounter() % TEMPERATURE_FREQUENCY_SEC == 0){
                                String[] row = generateRow(room, sensor, "temperature", currentTime);
                                sensor.setSensorChannel(sensor.getSensorChannel() + 1);

                                bwDataSource1.write(row[0]);
                                bwDataSource2.write(row[1]);
                            }
                            if(sensor.getTimeCounter() % BATTERY_FREQUENCY_SEC == 0){
                                String[] row = generateRow(room, sensor, "battery", currentTime);
                                sensor.setSensorChannel(sensor.getSensorChannel() + 1);

                                bwDataSource1.write(row[0]);
                                bwDataSource2.write(row[1]);
                            }
                            if(sensor.getTimeCounter() == PRESENCE_FREQUENCY_SEC){
                                String[] row = generateRow(room, sensor, "presence", currentTime);
                                sensor.setPresent(rand.nextBoolean());
                                sensor.setTimeCounter(0);
                                sensor.setSensorChannel(sensor.getSensorChannel() + 1);

                                bwDataSource1.write(row[0]);
                                bwDataSource2.write(row[1]);
                            }

                            sensor.setTimeCounter(sensor.getTimeCounter() + 1);
                            sensor.setTemperature(sensor.getTemperature() + (-0.1 + 0.2 * rand.nextDouble()));
                            sensor.setBattery(sensor.getBattery() - (batteryExhaustion + 0.0001 * rand.nextDouble()));
                        }
                    }
                }

                currentTime += 1000;
            }

            bwDataSource1.close();
            bwDataSource2.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }


    private static String[] generateRow(Room room, Sensor sensor, String type, Long currentTimeLong){
        String dataSource1 = String.format("%s,%s,%s,%s \n", sensor.getSensorId(), sensor.getSensorChannel(), type, room.getRoomId());
        String dataSource2;

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = dateFormat.format(new Timestamp(currentTimeLong));

        if(type.equalsIgnoreCase("temperature")){
            dataSource2 = String.format("%s,%s,%s,%s \n", sensor.getSensorId(), sensor.getSensorChannel(), currentTime, sensor.getTemperature());
        }
        else if(type.equalsIgnoreCase("battery")){
            dataSource2 = String.format("%s,%s,%s,%s \n", sensor.getSensorId(), sensor.getSensorChannel(), currentTime, sensor.getBattery());
        }
        else{
            dataSource2 = String.format("%s,%s,%s,%s \n", sensor.getSensorId(), sensor.getSensorChannel(), currentTime, sensor.getPresent() ? 1 : 0);
        }

        return(new String[]{dataSource1, dataSource2});
    }


}