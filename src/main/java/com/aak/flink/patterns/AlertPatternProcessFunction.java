package com.aak.flink.patterns;

import com.aak.flink.events.TemperatureAlert;
import com.aak.flink.events.TemperatureWarning;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class AlertPatternProcessFunction extends PatternProcessFunction<TemperatureWarning, TemperatureAlert> {
    @Override
    public void processMatch(Map<String, List<TemperatureWarning>> match, Context ctx, Collector<TemperatureAlert> out) throws Exception {
        TemperatureWarning firstWarning = match.get(Patterns.firstWarning).get(0);
        TemperatureWarning secondWarning = match.get(Patterns.secondWarning).get(0);
        out.collect(new TemperatureAlert(firstWarning.getRackId(),
                (firstWarning.getTemperatureAvg() + secondWarning.getTemperatureAvg())/2));
    }
}
