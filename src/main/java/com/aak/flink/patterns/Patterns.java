package com.aak.flink.patterns;

import com.aak.flink.events.MonitoringEvent;
import com.aak.flink.events.TemperatureEvent;
import com.aak.flink.events.TemperatureWarning;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Patterns {

    private static final double TEMPERATURE_THRESHOLD = 100;

    public static final String firstEventInMonitoringPattern = "firstEvent";

    public static final String secondEventInMonitoringPattern = "secondEvent";

    public static final String firstWarning = "firstWarning";

    public static final String secondWarning = "secondWarning";

    public static Pattern<MonitoringEvent, ?> warningPattern = Pattern.<MonitoringEvent>begin(firstEventInMonitoringPattern)
            .subtype(TemperatureEvent.class)
            .where(new IterativeCondition<TemperatureEvent>() {
                @Override
                public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) throws Exception {
                    return value.getTemperature() >= TEMPERATURE_THRESHOLD;
                }
            }).next(secondEventInMonitoringPattern)
            .subtype(TemperatureEvent.class)
            .where(new IterativeCondition<TemperatureEvent>() {
                @Override
                public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) throws Exception {
                    return value.getTemperature() >= TEMPERATURE_THRESHOLD;
                }
            }).within(Time.seconds(10));

    public static Pattern<TemperatureWarning, ?> alertPattern = Pattern.<TemperatureWarning>begin(firstWarning)
            .next(secondWarning)
            .within(Time.seconds(10));
}
