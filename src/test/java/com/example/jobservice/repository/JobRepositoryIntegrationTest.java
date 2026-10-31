package com.example.jobservice.repository;

import com.example.jobservice.domain.*;
import com.example.jobservice.api.ApiKeyFilter;
import com.example.jobservice.worker.WorkerPoller;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties={"jobs.worker.enabled=false", "jobs.scheduler.enabled=false", "API_KEY="})
@AutoConfigureMockMvc
@Testcontainers
class JobRepositoryIntegrationTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("jobs").withUsername("jobs").withPassword("jobs");
    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired JobRepository jobs;
    @Autowired Handlers handlers;
    @Autowired io.micrometer.core.instrument.MeterRegistry meters;
    @Autowired ScheduleRepository schedules;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired MockMvc mvc;
    @Autowired WebApplicationContext webContext;

    @BeforeEach void cleanup() {
        jdbc.update("DELETE FROM dead_letters");
        jdbc.update("DELETE FROM job_executions");
        jdbc.update("DELETE FROM jobs");
        jdbc.update("DELETE FROM job_schedules");
        jobs.drain(false);
    }

    private JobRequest sumRequest(int retries, Instant runAt) throws Exception {
        return new JobRequest("sum", mapper.readTree("{\"numbers\":[2,5]}"), 10, retries, 30, runAt);
    }

    private Job awaitStatus(UUID id, String expected, WorkerPoller poller, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Job last;
        do {
            if (poller != null) poller.poll();
            last = jobs.getJob(id);
            if (expected.equals(last.status())) return last;
            Thread.sleep(25);
        } while (System.nanoTime() < deadline);
        return fail("Expected job " + id + " to become " + expected + ", but it was " + last.status());
    }

    private UUID submitThroughApi(String request) throws Exception {
        String response = mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(request))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.attemptCount").value(0))
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(mapper.readTree(response).get("id").asText());
    }


    @Test void realWorkerExecutesAnEndToEndJob() throws Exception {
        Job job = jobs.submit(sumRequest(0, null), null);
        var poller = new com.example.jobservice.worker.WorkerPoller(jobs, handlers, meters, 1, 15);
        poller.start();
        try {
            poller.poll();
            Instant deadline = Instant.now().plusSeconds(5);
            while (Instant.now().isBefore(deadline) &&
                    !"SUCCEEDED".equals(jobs.getJob(job.id()).status())) {
                Thread.sleep(50);
            }
            assertEquals("SUCCEEDED", jobs.getJob(job.id()).status());
            assertEquals(7, jobs.getJob(job.id()).result().get("sum").asInt());
            assertEquals(1, jobs.executions(job.id()).size());
        } finally {
            poller.stop();
        }
    }

    @Test void submitClaimAndCompleteWithExecutionHistory() throws Exception {
        Job job = jobs.submit(sumRequest(2, null), null);
        assertEquals("QUEUED", job.status());
        ClaimedJob claim = jobs.claim("worker-A", 15).orElseThrow();
        assertEquals(job.id(), claim.id());
        assertEquals(1, claim.attemptNumber());
        assertEquals("RUNNING", jobs.getJob(job.id()).status());
        assertTrue(jobs.complete(claim, mapper.readTree("{\"sum\":7}")));
        assertEquals("SUCCEEDED", jobs.getJob(job.id()).status());
        assertEquals(7, jobs.getJob(job.id()).result().get("sum").asInt());
        assertEquals(1, jobs.executions(job.id()).size());
        assertEquals("SUCCEEDED", jobs.executions(job.id()).getFirst().get("status"));
        assertFalse(jobs.complete(claim, mapper.readTree("{\"sum\":123}")));
    }

    @Test void failedAttemptRetriesThenDeadLetters() throws Exception {
        Job job = jobs.submit(sumRequest(1, null), null);
        ClaimedJob first = jobs.claim("w1", 15).orElseThrow();
        assertTrue(jobs.fail(first, "FAILED", "Transient error"));
        assertEquals("FAILED", jobs.getJob(job.id()).status());
        assertTrue(jobs.claim("w2", 15).isEmpty()); // Backoff has not elapsed.
        jdbc.update("UPDATE jobs SET available_at=now()-interval '1 second' WHERE id=?", job.id());
        ClaimedJob second = jobs.claim("w2", 15).orElseThrow();
        assertTrue(jobs.fail(second, "TIMED_OUT", "Deadline exceeded"));
        assertEquals("DEAD_LETTERED", jobs.getJob(job.id()).status());
        assertEquals(2, jobs.getJob(job.id()).attemptCount());
        assertEquals(2, jobs.executions(job.id()).size());
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM dead_letters WHERE job_id=?", Long.class, job.id()));
    }

    @Test void recoverExpiredLeaseAndFenceStaleWorker() throws Exception {
        Job job = jobs.submit(sumRequest(1, null), null);
        ClaimedJob old = jobs.claim("crashed", 15).orElseThrow();
        jdbc.update("UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=?", job.id());
        assertEquals(1, jobs.recoverExpired(10));
        assertEquals("LOST", jobs.executions(job.id()).getFirst().get("status"));
        assertFalse(jobs.complete(old, mapper.readTree("{\"sum\":7}")));
        jdbc.update("UPDATE jobs SET available_at=now()-interval '1 second' WHERE id=?", job.id());
        ClaimedJob next = jobs.claim("healthy", 15).orElseThrow();
        assertEquals(2, next.attemptNumber());
        assertTrue(jobs.complete(next, mapper.readTree("{\"sum\":7}")));
    }

    @Test void delayedJobCannotBeClaimedEarly() throws Exception {
        Job job = jobs.submit(sumRequest(0, Instant.now().plusSeconds(3600)), null);
        assertTrue(jobs.claim("w1", 15).isEmpty());
        assertEquals("QUEUED", jobs.getJob(job.id()).status());
        jdbc.update("UPDATE jobs SET available_at=now()-interval '1 second' WHERE id=?", job.id());
        assertTrue(jobs.claim("w1", 15).isPresent());
    }

    @Test void drainAndCancellation() throws Exception {
        Job job = jobs.submit(sumRequest(0, null), null);
        jobs.drain(true);
        assertTrue(jobs.claim("w", 15).isEmpty());
        assertTrue(jobs.cancel(job.id()));
        assertEquals("CANCELLED", jobs.getJob(job.id()).status());
        jobs.drain(false);
        assertTrue(jobs.claim("w", 15).isEmpty());
    }

    @Test void onlyOneWorkerCanClaimTheSameRow() throws Exception {
        Job job = jobs.submit(sumRequest(0, null), null);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        Callable<Optional<ClaimedJob>> action = () -> {
            ready.countDown();
            go.await();
            return jobs.claim(Thread.currentThread().getName(), 15);
        };
        try {
            var a = pool.submit(action);
            var b = pool.submit(action);
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();
            int successes = (a.get(10, TimeUnit.SECONDS).isPresent() ? 1 : 0) +
                    (b.get(10, TimeUnit.SECONDS).isPresent() ? 1 : 0);
            assertEquals(1, successes);
            assertEquals(1, jobs.getJob(job.id()).attemptCount());
        } finally { pool.shutdownNow(); }
    }

    @Test void idempotencyKeyIsReusedAndConflictsOnChangedPayload() throws Exception {
        Job first = jobs.submit(sumRequest(0, null), "client-123");
        assertEquals(first.id(), jobs.submit(sumRequest(0, null), "client-123").id());
        var other = new JobRequest("sum", mapper.readTree("{\"numbers\":[1]}"),10,0,30,null);
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> jobs.submit(other, "client-123"));
    }

    @Test void recurringScheduleMaterializesOnceAndAdvances() throws Exception {
        var req = new ScheduleRequest("sum", mapper.readTree("{\"numbers\":[1]}"),
                "0 * * * * *", "UTC", 0, 0, 30);
        var schedule = schedules.create(req);
        jdbc.update("UPDATE job_schedules SET next_run_at=now()-interval '1 minute' WHERE id=?", schedule.id());
        assertEquals(1, schedules.materializeDue(100));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM jobs WHERE schedule_id=?", Long.class, schedule.id()));
        assertTrue(schedules.get(schedule.id()).nextRunAt().isAfter(Instant.now().minusSeconds(60)));
        schedules.disable(schedule.id());
        assertFalse(schedules.get(schedule.id()).enabled());
    }

    @Test void restApiReturnsJobAndExecutionHistory() throws Exception {
        String response = mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"sum\",\"payload\":{\"numbers\":[1,2]},\"maxRetries\":1}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.status").value("QUEUED"))
                .andReturn().getResponse().getContentAsString();
        String id = mapper.readTree(response).get("id").asText();
        mvc.perform(get("/jobs/"+id)).andExpect(status().isOk()).andExpect(jsonPath("$.type").value("sum"));
        mvc.perform(get("/jobs/"+id+"/executions")).andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
        mvc.perform(get("/queue/depth")).andExpect(status().isOk())
                .andExpect(jsonPath("$.ready").value(1));
        mvc.perform(post("/jobs/"+id+"/cancel")).andExpect(status().isOk());
        mvc.perform(get("/jobs/"+id)).andExpect(jsonPath("$.status").value("CANCELLED"));
    }


    @Test void cancelPendingDoesNotCancelRunningJobs() throws Exception {
        Job running = jobs.submit(sumRequest(0, null), null);
        Job queued = jobs.submit(sumRequest(0, Instant.now().plusSeconds(3600)), null);
        ClaimedJob active = jobs.claim("w1", 15).orElseThrow();
        assertEquals(running.id(), active.id());
        assertEquals(1, jobs.cancelPending());
        assertEquals("RUNNING", jobs.getJob(running.id()).status());
        assertEquals("CANCELLED", jobs.getJob(queued.id()).status());
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> jobs.cancel(running.id()));
        assertTrue(jobs.complete(active, mapper.readTree("{\"sum\":7}")));
    }

    @Test void cronRequestRejectsInvalidTimezone() throws Exception {
        var request = new ScheduleRequest("sum", mapper.readTree("{\"numbers\":[1]}"),
                "0 0 22 * * *", "Not/AZone", 0, 0, 30);
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> schedules.create(request));
    }

    @Test void scheduleApiCreatesAndDisablesDefinition() throws Exception {
        String json = "{\"type\":\"sum\",\"payload\":{\"numbers\":[2]}," +
                "\"cronExpression\":\"0 0 22 * * *\",\"timezone\":\"Asia/Kolkata\"}";
        String response = mvc.perform(post("/schedules").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.enabled").value(true))
                .andReturn().getResponse().getContentAsString();
        String id = mapper.readTree(response).get("id").asText();
        mvc.perform(get("/schedules/" + id)).andExpect(status().isOk());
        mvc.perform(delete("/schedules/" + id)).andExpect(status().isOk());
        mvc.perform(get("/schedules/" + id)).andExpect(jsonPath("$.enabled").value(false));
    }

    @Test void rejectsInvalidSubmission() throws Exception {
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"type\":\"unknown\",\"payload\":{}}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"type\":\"sum\",\"payload\":{},\"timeoutSeconds\":0}"))
                .andExpect(status().isBadRequest());
    }

    @Test void higherPriorityRunsFirstWithoutClaimingFutureWork() throws Exception {
        var payload = mapper.readTree("{\"numbers\":[1]}");
        Job low = jobs.submit(new JobRequest("sum", payload, -10, 0, 30, null), null);
        Job high = jobs.submit(new JobRequest("sum", payload, 20, 0, 30, null), null);
        Job future = jobs.submit(new JobRequest("sum", payload, 100, 0, 30,
                Instant.now().plusSeconds(3600)), null);

        assertEquals(high.id(), jobs.claim("priority-worker", 15).orElseThrow().id());
        assertEquals(low.id(), jobs.claim("priority-worker", 15).orElseThrow().id());
        assertTrue(jobs.claim("priority-worker", 15).isEmpty());
        assertEquals("QUEUED", jobs.getJob(future.id()).status());
        assertEquals(0, jobs.getJob(future.id()).attemptCount());
    }

    @Test void realWorkerRetriesTransientFailureAndHttpShowsFinalResult() throws Exception {
        UUID id = submitThroughApi("""
                {"type":"flaky","payload":{"failUntilAttempt":1},"maxRetries":1,"timeoutSeconds":5}
                """);
        var poller = new WorkerPoller(jobs, handlers, new SimpleMeterRegistry(), 1, 15);
        poller.start();
        try {
            poller.poll();
            Job failed = awaitStatus(id, "FAILED", null, Duration.ofSeconds(5));
            mvc.perform(get("/jobs/" + id)).andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("FAILED"))
                    .andExpect(jsonPath("$.attemptCount").value(1))
                    .andExpect(jsonPath("$.lastError").value(
                            org.hamcrest.Matchers.containsString("Simulated transient failure")));
            assertTrue(failed.availableAt().isAfter(failed.createdAt()));

            // Exercise the actual retry delay; do not rewrite available_at to bypass backoff.
            Job succeeded = awaitStatus(id, "SUCCEEDED", poller, Duration.ofSeconds(6));
            assertEquals(2, succeeded.attemptCount());
            mvc.perform(get("/jobs/" + id)).andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                    .andExpect(jsonPath("$.attemptCount").value(2))
                    .andExpect(jsonPath("$.lastError").isEmpty())
                    .andExpect(jsonPath("$.result.completedOnAttempt").value(2));
            mvc.perform(get("/jobs/" + id + "/executions")).andExpect(status().isOk())
                    .andExpect(jsonPath("$.length()").value(2))
                    .andExpect(jsonPath("$[0].status").value("FAILED"))
                    .andExpect(jsonPath("$[1].status").value("SUCCEEDED"))
                    .andExpect(jsonPath("$[1].result.completedOnAttempt").value(2));
            assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM dead_letters WHERE job_id=?", Long.class, id));
        } finally {
            poller.stop();
        }
    }

    @Test void actualWorkerTimeoutIsVisibleInHttpAndDeadLetterStore() throws Exception {
        UUID id = submitThroughApi("""
                {"type":"sleep","payload":{"millis":5000},"maxRetries":0,"timeoutSeconds":1}
                """);
        var poller = new WorkerPoller(jobs, handlers, new SimpleMeterRegistry(), 1, 15);
        poller.start();
        try {
            poller.poll();
            mvc.perform(get("/jobs/" + id)).andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("RUNNING"))
                    .andExpect(jsonPath("$.attemptCount").value(1));
            awaitStatus(id, "DEAD_LETTERED", null, Duration.ofSeconds(5));
            mvc.perform(get("/jobs/" + id)).andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("DEAD_LETTERED"))
                    .andExpect(jsonPath("$.attemptCount").value(1))
                    .andExpect(jsonPath("$.result").isEmpty())
                    .andExpect(jsonPath("$.lastError").value(org.hamcrest.Matchers.containsString("exceeded 1 seconds")));
            mvc.perform(get("/jobs/" + id + "/executions"))
                    .andExpect(jsonPath("$.length()").value(1))
                    .andExpect(jsonPath("$[0].status").value("TIMED_OUT"))
                    .andExpect(jsonPath("$[0].finishedAt").isNotEmpty());
            assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM dead_letters WHERE job_id=?", Long.class, id));
            assertTrue(jobs.claim("another-worker", 15).isEmpty());
        } finally {
            poller.stop();
        }
    }

    @Test void realWorkerExhaustsRetryBudgetAndKeepsEveryFailedAttempt() throws Exception {
        UUID id = submitThroughApi("""
                {"type":"flaky","payload":{"failUntilAttempt":10},"maxRetries":1,"timeoutSeconds":5}
                """);
        var poller = new WorkerPoller(jobs, handlers, new SimpleMeterRegistry(), 1, 15);
        poller.start();
        try {
            awaitStatus(id, "DEAD_LETTERED", poller, Duration.ofSeconds(7));
            assertEquals(2, jobs.getJob(id).attemptCount());
            assertEquals(List.of("FAILED", "FAILED"),
                    jobs.executions(id).stream().map(execution -> execution.get("status")).toList());
            assertEquals(2, jdbc.queryForObject("SELECT attempt_count FROM dead_letters WHERE job_id=?", Integer.class, id));
            assertTrue(jobs.claim("another-worker", 15).isEmpty());
        } finally {
            poller.stop();
        }
    }

    @Test void concurrentSchedulersMaterializeOneJobForTheSameOccurrence() throws Exception {
        var schedule = schedules.create(new ScheduleRequest("sum", mapper.readTree("{\"numbers\":[1]}"),
                "0 0 0 * * *", "UTC", 0, 0, 30));
        Instant occurrence = Instant.now().atZone(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant();
        jdbc.update("UPDATE job_schedules SET next_run_at=? WHERE id=?", Timestamp.from(occurrence), schedule.id());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        Callable<Integer> materialize = () -> {
            ready.countDown();
            assertTrue(go.await(5, TimeUnit.SECONDS));
            return schedules.materializeDue(10);
        };
        try {
            Future<Integer> a = pool.submit(materialize);
            Future<Integer> b = pool.submit(materialize);
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();
            assertEquals(1, a.get(10, TimeUnit.SECONDS) + b.get(10, TimeUnit.SECONDS));
            assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM jobs WHERE schedule_id=?", Long.class, schedule.id()));
            assertEquals(occurrence, jdbc.queryForObject("SELECT scheduled_for FROM jobs WHERE schedule_id=?",
                    Timestamp.class, schedule.id()).toInstant());
            assertEquals(occurrence.plus(Duration.ofDays(1)), schedules.get(schedule.id()).nextRunAt());
            assertEquals(0, schedules.materializeDue(10));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test void disablingScheduleCancelsPendingOccurrencesButPreservesRunningWork() throws Exception {
        var schedule = schedules.create(new ScheduleRequest("sum", mapper.readTree("{\"numbers\":[1]}"),
                "0 0 0 * * *", "UTC", 0, 1, 30));
        Instant midnight = Instant.now().atZone(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant();
        jdbc.update("UPDATE job_schedules SET next_run_at=? WHERE id=?",
                Timestamp.from(midnight.minus(Duration.ofDays(2))), schedule.id());
        assertEquals(1, schedules.materializeDue(10));
        ClaimedJob retry = jobs.claim("retrying-worker", 15).orElseThrow();
        assertTrue(jobs.fail(retry, "FAILED", "temporary error"));
        jdbc.update("UPDATE jobs SET available_at=now()+interval '1 hour' WHERE id=?", retry.id());
        assertEquals(1, schedules.materializeDue(10));
        ClaimedJob running = jobs.claim("running-worker", 15).orElseThrow();
        assertEquals(1, schedules.materializeDue(10));

        mvc.perform(delete("/schedules/" + schedule.id())).andExpect(status().isOk());
        assertFalse(schedules.get(schedule.id()).enabled());
        assertEquals(2L, jdbc.queryForObject("SELECT count(*) FROM jobs WHERE schedule_id=? AND status='CANCELLED'",
                Long.class, schedule.id()));
        assertEquals("RUNNING", jobs.getJob(running.id()).status());
        assertEquals("CANCELLED", jobs.getJob(retry.id()).status());
        assertTrue(jobs.complete(running, mapper.readTree("{\"sum\":1}")));
        assertEquals(0, schedules.materializeDue(10));
        assertTrue(jobs.claim("idle-worker", 15).isEmpty());
    }

    @Test void completionRacingRecoveryProducesOneConsistentOutcome() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < 8; iteration++) {
                Job submitted = jobs.submit(sumRequest(0, null), null);
                ClaimedJob claim = jobs.claim("finishing-worker", 15).orElseThrow();
                jdbc.update("UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=?", submitted.id());
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                Future<Boolean> completion = pool.submit(() -> {
                    ready.countDown();
                    assertTrue(go.await(5, TimeUnit.SECONDS));
                    return jobs.complete(claim, mapper.readTree("{\"sum\":7}"));
                });
                Future<Integer> recovery = pool.submit(() -> {
                    ready.countDown();
                    assertTrue(go.await(5, TimeUnit.SECONDS));
                    return jobs.recoverExpired(10);
                });
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                go.countDown();
                boolean completed = completion.get(10, TimeUnit.SECONDS);
                assertEquals(completed ? 0 : 1, recovery.get(10, TimeUnit.SECONDS));
                assertEquals(completed ? "SUCCEEDED" : "DEAD_LETTERED", jobs.getJob(submitted.id()).status());
                assertEquals(1, jobs.executions(submitted.id()).size());
                assertEquals(completed ? "SUCCEEDED" : "LOST", jobs.executions(submitted.id()).getFirst().get("status"));
                assertEquals(completed ? 0L : 1L, jdbc.queryForObject("SELECT count(*) FROM dead_letters WHERE job_id=?",
                        Long.class, submitted.id()));
                assertFalse(jobs.complete(claim, mapper.readTree("{\"sum\":99}")));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test void crashAfterClaimWithNoRetriesRecordsUnknownOutcomeWithoutExecutingHandler() throws Exception {
        Job job = jobs.submit(sumRequest(0, null), null);
        ClaimedJob claim = jobs.claim("crashed-before-dispatch", 15).orElseThrow();
        // No handler is invoked: a committed claim itself consumes the finite attempt budget.
        jdbc.update("UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=?", job.id());
        assertEquals(1, jobs.recoverExpired(10));
        assertEquals("DEAD_LETTERED", jobs.getJob(job.id()).status());
        assertEquals(1, jobs.getJob(job.id()).attemptCount());
        assertNull(jobs.getJob(job.id()).result());
        assertEquals("LOST", jobs.executions(job.id()).getFirst().get("status"));
        assertTrue(jobs.getJob(job.id()).lastError().contains("outcome unknown"));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM dead_letters WHERE job_id=?", Long.class, job.id()));
        assertTrue(jobs.claim("replacement-worker", 15).isEmpty());
        assertFalse(jobs.complete(claim, mapper.readTree("{\"sum\":7}")));
        assertEquals(0, jobs.recoverExpired(10));
    }

    @Test void queueHttpControlsPauseResumeAndCancelOnlyPendingJobs() throws Exception {
        Job runningJob = jobs.submit(sumRequest(1, null), null);
        ClaimedJob running = jobs.claim("active-worker", 15).orElseThrow();
        Job retryJob = jobs.submit(sumRequest(1, null), null);
        ClaimedJob retry = jobs.claim("retry-worker", 15).orElseThrow();
        assertTrue(jobs.fail(retry, "FAILED", "retry me"));
        jdbc.update("UPDATE jobs SET available_at=now()+interval '1 hour' WHERE id=?", retryJob.id());
        Job ready = jobs.submit(sumRequest(0, null), null);
        Job delayed = jobs.submit(sumRequest(0, Instant.now().plusSeconds(3600)), null);

        mvc.perform(post("/queue/drain")).andExpect(status().isOk())
                .andExpect(jsonPath("$.draining").value(true));
        assertTrue(jobs.claim("paused-worker", 15).isEmpty());
        mvc.perform(get("/queue/depth")).andExpect(status().isOk())
                .andExpect(jsonPath("$.draining").value(true))
                .andExpect(jsonPath("$.ready").value(1))
                .andExpect(jsonPath("$.delayed").value(1))
                .andExpect(jsonPath("$.retry_wait").value(1))
                .andExpect(jsonPath("$.running").value(1));
        assertTrue(jobs.complete(running, mapper.readTree("{\"sum\":7}")));
        mvc.perform(post("/queue/resume")).andExpect(status().isOk())
                .andExpect(jsonPath("$.draining").value(false));
        ClaimedJob resumed = jobs.claim("resumed-worker", 15).orElseThrow();
        assertEquals(ready.id(), resumed.id());
        mvc.perform(post("/queue/cancel-pending")).andExpect(status().isOk())
                .andExpect(jsonPath("$.cancelledCount").value(2));
        assertEquals("SUCCEEDED", jobs.getJob(runningJob.id()).status());
        assertEquals("RUNNING", jobs.getJob(ready.id()).status());
        assertEquals("CANCELLED", jobs.getJob(retryJob.id()).status());
        assertEquals("CANCELLED", jobs.getJob(delayed.id()).status());
        mvc.perform(post("/jobs/" + ready.id() + "/cancel")).andExpect(status().isConflict());
        assertTrue(jobs.complete(resumed, mapper.readTree("{\"sum\":7}")));
    }

    @Test void apiKeyProtectsJobAndMetricsRoutesWhileHealthRemainsAvailable() throws Exception {
        Job job = jobs.submit(sumRequest(0, null), null);
        MockMvc protectedMvc = MockMvcBuilders.webAppContextSetup(webContext)
                .addFilters(new ApiKeyFilter("integration-test-secret")).build();
        for (String path : List.of("/jobs/" + job.id(), "/queue/depth", "/handlers",
                "/schedules/" + UUID.randomUUID(), "/actuator/metrics")) {
            protectedMvc.perform(get(path)).andExpect(status().isUnauthorized());
            protectedMvc.perform(get(path).header("X-API-Key", "incorrect"))
                    .andExpect(status().isUnauthorized());
        }
        protectedMvc.perform(get("/jobs/" + job.id()).header("X-API-Key", "integration-test-secret"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(job.id().toString()));
        mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk());
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
    }

    @Test void httpRejectsUnknownJobsMalformedIdsAndInvalidSchedulingParameters() throws Exception {
        UUID missing = UUID.randomUUID();
        mvc.perform(get("/jobs/" + missing)).andExpect(status().isNotFound());
        mvc.perform(get("/jobs/" + missing + "/executions")).andExpect(status().isNotFound());
        mvc.perform(post("/jobs/" + missing + "/cancel")).andExpect(status().isNotFound());
        mvc.perform(get("/jobs/not-a-uuid")).andExpect(status().isBadRequest());
        for (String extra : List.of("\"priority\":101", "\"maxRetries\":-1", "\"timeoutSeconds\":86401")) {
            mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                    .content("{\"type\":\"sum\",\"payload\":{\"numbers\":[1]}," + extra + "}"))
                    .andExpect(status().isBadRequest());
        }
        mvc.perform(post("/schedules").contentType(MediaType.APPLICATION_JSON).content("""
                {"type":"sum","payload":{"numbers":[1]},"cronExpression":"invalid","timezone":"UTC"}
                """)).andExpect(status().isBadRequest());
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", "x".repeat(161))
                .content("{\"type\":\"sum\",\"payload\":{\"numbers\":[1]}}"))
                .andExpect(status().isBadRequest());
    }
}
