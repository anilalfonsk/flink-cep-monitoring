package com.aak.flink.eventsource;

import com.aak.flink.events.MonitoringEvent;
import com.aak.flink.events.PowerEvent;
import com.aak.flink.events.TemperatureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class MonitoringEventSource extends RichParallelSourceFunction<MonitoringEvent> {

    private boolean running = true;

    private final int maxRackId;

    private final long pause;

    private final double temperatureRatio;

    private final double powerStd;

    private final double powerMean;

    private final double temperatureStd;

    private final double temperatureMean;

    private int shard;

    private int offset;

    public MonitoringEventSource(
            int maxRackId,
            long pause,
            double temperatureRatio,
            double powerStd,
            double powerMean,
            double temperatureStd,
            double temperatureMean) {
        this.maxRackId = maxRackId;
        this.pause = pause;
        this.temperatureRatio = temperatureRatio;
        this.powerMean = powerMean;
        this.powerStd = powerStd;
        this.temperatureMean = temperatureMean;
        this.temperatureStd = temperatureStd;
    }

    @Override
    public void open(Configuration configuration) {
        int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int index = getRuntimeContext().getIndexOfThisSubtask();

        offset = (int)((double)maxRackId / numberTasks * index);
        shard = (int)((double)maxRackId / numberTasks * (index + 1)) - offset;
    }

    public void run(SourceContext<MonitoringEvent> sourceContext) throws Exception {
        while (running) {
            MonitoringEvent monitoringEvent;

            final ThreadLocalRandom random = ThreadLocalRandom.current();

            if (shard > 0) {
                int rackId = random.nextInt(shard) + offset;

                if (random.nextDouble() >= temperatureRatio) {
                    double power = random.nextGaussian() * powerStd + powerMean;
                    monitoringEvent = PowerEvent.builder()
                            .rackID(rackId)
                            .voltage(power)
                            .build();
                } else {
                    double temperature = random.nextGaussian() * temperatureStd + temperatureMean;
                    monitoringEvent = TemperatureEvent.builder().temperature(temperature).rackID(rackId).build();
                }
                sourceContext.collect(monitoringEvent);
            }

            Thread.sleep(pause);
        }
    }

    public void cancel() {
        running = false;
    }
}
