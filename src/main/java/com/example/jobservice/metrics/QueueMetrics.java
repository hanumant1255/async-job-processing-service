package com.example.jobservice.metrics;

import com.example.jobservice.repository.JobRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class QueueMetrics {
    public QueueMetrics(MeterRegistry meters, JobRepository jobs) {
        meters.gauge("jobs.queue.depth", jobs, JobRepository::readyCount);
    }
}
