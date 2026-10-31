package com.example.jobservice.worker;

import com.example.jobservice.domain.*;
import com.example.jobservice.repository.JobRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WorkerPollerTest {
    @Test void timesOutInterruptibleHandlerAndRecordsFailure() throws Exception {
        JobRepository repository = mock(JobRepository.class);
        ObjectMapper mapper = new ObjectMapper();
        Handlers handlers = new Handlers(java.util.List.of(new SleepJobHandler(mapper)));
        ClaimedJob claim = new ClaimedJob(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "sleep", mapper.readTree("{\"millis\":5000}"), 1, 0, 1);
        when(repository.claim(anyString(), anyInt()))
                .thenReturn(Optional.of(claim), Optional.empty());
        when(repository.fail(eq(claim), eq("TIMED_OUT"), anyString())).thenReturn(true);
        WorkerPoller poller = new WorkerPoller(repository, handlers, new SimpleMeterRegistry(), 1, 2);
        poller.start();
        try {
            poller.poll();
            verify(repository, timeout(4000)).fail(eq(claim), eq("TIMED_OUT"), contains("exceeded"));
            verify(repository, never()).complete(eq(claim), any());
        } finally {
            poller.stop();
        }
    }
}
