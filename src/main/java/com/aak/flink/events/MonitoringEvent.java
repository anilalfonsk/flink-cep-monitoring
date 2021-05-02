package com.aak.flink.events;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public abstract class MonitoringEvent {
    private int rackID;

//    @Override
//    public boolean equals(Object o){
//        if(o instanceof MonitoringEvent) {
//            MonitoringEvent monitoringEvent = (MonitoringEvent) o;
//            return (monitoringEvent instanceof MonitoringEvent) && (rackID == monitoringEvent.getRackID());
//        }else {
//            return false;
//        }
//    }
}
