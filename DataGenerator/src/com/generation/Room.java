package com.generation;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Random;

public class Room {

    Room(int maxRoomSize, double roomTemperature, int timeCounter){

        setRoomId(UUID.randomUUID());

        Random rand = new Random();
        for(int i = 0; i < rand.nextInt(maxRoomSize) + 1; i++){

            setRoomSensors(new Sensor(rand.nextBoolean(), roomTemperature, timeCounter));
        }
    }


    public UUID getRoomId() {
        return roomId;
    }

    private void setRoomId(UUID roomId) {
        this.roomId = roomId;
    }

    public List<Sensor> getRoomSensors() {
        return roomSensors;
    }

    private void setRoomSensors(Sensor roomSensor) {
        this.roomSensors.add(roomSensor);
    }

    private UUID roomId;
    private List<Sensor> roomSensors = new ArrayList<Sensor>();
}
