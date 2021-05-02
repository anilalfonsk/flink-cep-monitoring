package com.aak.flink.events;

import lombok.Data;

@Data
public class TemperatureWarning {
    private int rackId;
    private double temperatureAvg;
}
