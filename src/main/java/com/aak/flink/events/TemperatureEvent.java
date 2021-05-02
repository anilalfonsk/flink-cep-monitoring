package com.aak.flink.events;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class TemperatureEvent extends MonitoringEvent {
    private double temperature;

//    @Override
//    public boolean equals(Object obj){
//        if(obj instanceof TemperatureEvent){
//            TemperatureEvent temperatureEvent = (TemperatureEvent) obj;
//            return (temperatureEvent instanceof TemperatureEvent)
//                    && temperature == temperatureEvent.getTemperature()
//                    && super.equals(obj);
//        }else {
//            return false;
//        }
//    }
}
