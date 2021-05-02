package com.aak.flink.events;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TemperatureAlert {
    private int rackId;
    private double averageTemperature;
}
