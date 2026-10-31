package com.example.jobservice.worker;

import com.example.jobservice.domain.*;
import com.example.jobservice.repository.JobRepository;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name="jobs.worker.enabled", havingValue="true")
public class WorkerPoller {
    private static final Logger log = LoggerFactory.getLogger(WorkerPoller.class);
    private final JobRepository jobs;
    private final Handlers handlers;
    private final MeterRegistry metrics;
    private final int concurrency;
    private final int graceSeconds;
    private final AtomicInteger active = new AtomicInteger();
    private final String workerId;
    private ExecutorService dispatchPool;
    private ThreadPoolExecutor handlerPool;

    public WorkerPoller(JobRepository jobs, Handlers handlers, MeterRegistry metrics,
                        @Value("${jobs.worker.concurrency:4}") int concurrency,
                        @Value("${jobs.worker.lease-grace-seconds:15}") int graceSeconds) {
        if (concurrency < 1 || concurrency > 256) throw new IllegalArgumentException("Invalid worker concurrency");
        this.jobs = jobs;
        this.handlers = handlers;
        this.metrics = metrics;
        this.concurrency = concurrency;
        this.graceSeconds = graceSeconds;
        this.workerId = hostname() + "-" + UUID.randomUUID();
        metrics.gauge("jobs.worker.active", active);
        metrics.gauge("jobs.worker.utilization", active, value -> value.get() / (double) this.concurrency);
    }

    private static String hostname() {
        try { return InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { return "worker"; }
    }

    @PostConstruct
    public void start() {
        dispatchPool = Executors.newFixedThreadPool(concurrency);
        // A timed-out handler may ignore interrupts. Bound threads and reject new work rather than grow indefinitely.
        handlerPool = new ThreadPoolExecutor(concurrency, concurrency * 2, 30, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    }

    @Scheduled(fixedDelayString="${jobs.worker.poll-ms:250}")
    public void poll() {
        while (true) {
            int existing = active.get();
            if (existing >= concurrency || !active.compareAndSet(existing, existing + 1)) {
                if (existing >= concurrency) return;
                continue;
            }
            try {
                var claimed = jobs.claim(workerId, graceSeconds);
                if (claimed.isEmpty()) { active.decrementAndGet(); return; }
                dispatchPool.execute(() -> {
                    try { execute(claimed.get()); }
                    finally { active.decrementAndGet(); }
                });
            } catch (Exception e) {
                active.decrementAndGet();
                log.error("Could not claim or dispatch job", e);
                return;
            }
        }
    }

    private void execute(ClaimedJob job) {
        Timer.Sample sample = Timer.start(metrics);
        String outcome = "failed";
        Future<JsonNode> task = null;
        try {
            task = handlerPool.submit(() -> handlers.run(job.type(), job.payload(), new JobContext(job.id(), job.executionId(), job.attemptNumber())));
            JsonNode result = task.get(job.timeoutSeconds(), TimeUnit.SECONDS);
            if (jobs.complete(job, result)) outcome = "succeeded";
            else outcome = "stale";
        } catch (TimeoutException e) {
            if (task != null) task.cancel(true);
            if (jobs.fail(job, "TIMED_OUT", "Attempt exceeded " + job.timeoutSeconds() + " seconds"))
                outcome = "timed_out";
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (jobs.fail(job, "FAILED", cause.getClass().getSimpleName() + ": " + cause.getMessage()))
                outcome = "failed";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (task != null) task.cancel(true);
            jobs.fail(job, "FAILED", "Worker interrupted");
            outcome = "interrupted";
        } catch (RejectedExecutionException e) {
            jobs.fail(job, "FAILED", "Handler capacity exhausted");
            outcome = "capacity_exhausted";
        } catch (Exception e) {
            log.error("Failure while completing job {}", job.id(), e);
            // If the database is unavailable, keep the lease for the recovery scanner.
            outcome = "completion_error";
        } finally {
            sample.stop(metrics.timer("jobs.execution.duration", "outcome", outcome));
        }
    }

    @Scheduled(fixedDelayString="${jobs.worker.recovery-ms:2000}")
    public void recover() {
        try {
            int recovered = jobs.recoverExpired(50);
            if (recovered > 0) log.warn("Recovered {} expired job leases", recovered);
        } catch (Exception e) { log.error("Lease recovery failed", e); }
    }

    @PreDestroy
    public void stop() {
        if (dispatchPool != null) dispatchPool.shutdownNow();
        if (handlerPool != null) handlerPool.shutdownNow();
    }
}
