package com.aak.flink.events;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TemperatureWarning {
    private int rackId;
    private double temperatureAvg;
}
