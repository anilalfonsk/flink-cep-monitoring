package com.aak.flink.patterns;

import com.aak.flink.events.MonitoringEvent;
import com.aak.flink.events.TemperatureEvent;
import com.aak.flink.events.TemperatureWarning;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class TemperatureWarningPatternProcessFunction extends PatternProcessFunction <MonitoringEvent, TemperatureWarning>{
    @Override
    public void processMatch(Map<String, List<MonitoringEvent>> match, Context ctx, Collector<TemperatureWarning> out) throws Exception {
        TemperatureEvent firstEvent = (TemperatureEvent) match.get(Patterns.firstEventInMonitoringPattern).get(0);
        TemperatureEvent secondEvent = (TemperatureEvent) match.get(Patterns.secondEventInMonitoringPattern).get(0);
        out.collect(new TemperatureWarning(firstEvent.getRackID(),
                (firstEvent.getTemperature() + secondEvent.getTemperature())/2));
    }
}
