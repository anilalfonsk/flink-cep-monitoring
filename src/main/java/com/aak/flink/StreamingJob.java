/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aak.flink;

import com.aak.flink.events.MonitoringEvent;
import com.aak.flink.events.TemperatureAlert;
import com.aak.flink.events.TemperatureEvent;
import com.aak.flink.events.TemperatureWarning;
import com.aak.flink.eventsource.MonitoringEventSource;
import com.aak.flink.patterns.Patterns;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;

public class StreamingJob {

	private static final int MAX_RACK_ID = 10;
	private static final long PAUSE = 100;
	private static final double TEMPERATURE_RATIO = 0.5;
	private static final double POWER_STD = 10;
	private static final double POWER_MEAN = 100;
	private static final double TEMP_STD = 20;
	private static final double TEMP_MEAN = 80;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Input stream of monitoring events created using the monitoring source.
		DataStream<MonitoringEvent> inputEventStream = env
				.addSource(new MonitoringEventSource(
						MAX_RACK_ID,
						PAUSE,
						TEMPERATURE_RATIO,
						POWER_STD,
						POWER_MEAN,
						TEMP_STD,
						TEMP_MEAN))
				.assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

		PatternStream<MonitoringEvent> monitoringEventPatternStream = CEP.pattern(
				inputEventStream.keyBy(MonitoringEvent::getRackID), Patterns.warningPattern);

		DataStream<TemperatureWarning> warningDataStream = monitoringEventPatternStream.select(
				(PatternSelectFunction<MonitoringEvent, TemperatureWarning>) pattern -> {
			TemperatureEvent firstEvent = (TemperatureEvent) pattern.get(Patterns.firstEventInMonitoringPattern).get(0);
			TemperatureEvent secondEvent = (TemperatureEvent) pattern.get(Patterns.secondEventInMonitoringPattern).get(0);
			return new TemperatureWarning(firstEvent.getRackID(), (firstEvent.getTemperature() + secondEvent.getTemperature())/2);
		});

		PatternStream<TemperatureWarning> temperatureAlertPatternStream = CEP.pattern(warningDataStream
				.keyBy((KeySelector<TemperatureWarning, Integer>) TemperatureWarning::getRackId), Patterns.alertPattern);

		DataStream<TemperatureAlert> alertDataStream = temperatureAlertPatternStream.select(
				(PatternSelectFunction<TemperatureWarning, TemperatureAlert>) pattern -> {
					TemperatureWarning firstWarning = pattern.get(Patterns.firstWarning).get(0);
					TemperatureWarning secondWarning = pattern.get(Patterns.secondWarning).get(0);
					return new TemperatureAlert(firstWarning.getRackId(),
							(firstWarning.getTemperatureAvg() + secondWarning.getTemperatureAvg())/2);
				});

		alertDataStream.print();


		env.execute("Flink Streaming Java API Skeleton");
	}
}
