package com.leetjourney.ingestion_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IngestionServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionServiceApplication.class, args);
	}

}
// Java automatically removes objects that are no longer used.
//
//Example:
//
//Point point = Point.measurement(...);
//
//After writing to InfluxDB:
//
//writePoint(point);
//
//the point object is no longer needed.
//
//Java later removes it automatically.
//Every Kafka message creates objects:
//
//EnergyUsageEvent
//Point
//String
//Double
//Instant
//
//These are short-lived.
//
//They go into the Young Generation.
//
//When it becomes full:
//
//Young Generation Full
//
//↓
//
//Minor GC runs
//
//↓
//
//Unused objects removed
//
//↓
//
//Space becomes free
//
//Minor GC is very fast.
//
//Usually just a few milliseconds.