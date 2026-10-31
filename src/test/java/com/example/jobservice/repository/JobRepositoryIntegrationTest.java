package com.example.jobservice.repository;

import com.example.jobservice.domain.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties={"jobs.worker.enabled=false", "jobs.scheduler.enabled=false"})
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
}
