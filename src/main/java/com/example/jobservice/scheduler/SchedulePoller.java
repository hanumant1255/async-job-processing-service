package com.example.jobservice.scheduler;

import com.example.jobservice.repository.ScheduleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name="jobs.scheduler.enabled", havingValue="true")
public class SchedulePoller {
    private static final Logger log = LoggerFactory.getLogger(SchedulePoller.class);
    private final ScheduleRepository repository;
    private final int batchSize;

    public SchedulePoller(ScheduleRepository repository, @Value("${jobs.scheduler.batch-size:100}") int batchSize) {
        this.repository = repository;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString="${jobs.scheduler.poll-ms:1000}")
    public void poll() {
        int created = repository.materializeDue(batchSize);
        if (created > 0) log.info("Materialized {} scheduled job occurrences", created);
    }
}
