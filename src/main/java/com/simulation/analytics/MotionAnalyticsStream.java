package com.simulation.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
@EnableConfigurationProperties(AppProperties.class)
public class MotionAnalyticsStream {

    public static void main(String[] args) {
        SpringApplication.run(MotionAnalyticsStream.class, args);
    }
}
