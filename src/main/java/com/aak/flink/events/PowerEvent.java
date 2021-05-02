package com.aak.flink.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class PowerEvent extends MonitoringEvent{
    private double voltage;

//    @Override
//    public boolean equals(Object obj){
//        if(obj instanceof PowerEvent){
//            PowerEvent powerEvent = (PowerEvent) obj;
//            return powerEvent instanceof PowerEvent
//                    && super.equals(obj)
//                    && voltage == powerEvent.voltage;
//        }else {
//            return false;
//        }
//    }
}
