package com.example.jobservice.repository;

import com.example.jobservice.domain.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class JobRepository {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper json;
    private final MeterRegistry metrics;

    public JobRepository(JdbcTemplate jdbc, org.springframework.transaction.PlatformTransactionManager manager,
                         ObjectMapper json, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(manager);
        this.json = json;
        this.metrics = metrics;
    }

    private String stringify(JsonNode node) {
        try { return node == null ? null : json.writeValueAsString(node); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("Invalid JSON", e); }
    }

    private JsonNode parse(String value) {
        if (value == null) return null;
        try { return json.readTree(value); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Corrupt persisted JSON", e); }
    }

    private Job mapJob(ResultSet rs, int row) throws SQLException {
        return new Job((UUID) rs.getObject("id"), (UUID) rs.getObject("schedule_id"),
                instant(rs, "scheduled_for"), rs.getString("type"), parse(rs.getString("payload")),
                rs.getString("status"), rs.getInt("priority"), rs.getInt("attempt_count"),
                rs.getInt("max_retries"), rs.getInt("timeout_seconds"), instant(rs, "available_at"),
                parse(rs.getString("result")), rs.getString("last_error"),
                instant(rs, "created_at"), instant(rs, "updated_at"));
    }

    private static Instant instant(ResultSet rs, String col) throws SQLException {
        java.sql.Timestamp timestamp = rs.getTimestamp(col);
        return timestamp == null ? null : timestamp.toInstant();
    }

    public Job getJob(UUID id) {
        List<Job> result = jdbc.query("SELECT * FROM jobs WHERE id=?", this::mapJob, id);
        if (result.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found");
        return result.getFirst();
    }

    public Job submit(JobRequest request, String idempotencyKey) {
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > 160))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid Idempotency-Key");
        return Objects.requireNonNull(tx.execute(status -> {
            UUID id = UUID.randomUUID();
            List<UUID> inserted = jdbc.query("""
                    INSERT INTO jobs(id,type,payload,status,priority,max_retries,timeout_seconds,available_at,scheduled_for,idempotency_key)
                    VALUES (?, ?, ?::jsonb, 'QUEUED', ?, ?, ?, COALESCE(?,now()), ?, ?)
                    ON CONFLICT (idempotency_key) DO NOTHING RETURNING id
                    """, (rs, n) -> (UUID) rs.getObject(1), id, request.type(), stringify(request.payload()),
                    request.effectivePriority(), request.effectiveMaxRetries(), request.effectiveTimeoutSeconds(),
                    request.runAt() == null ? null : java.sql.Timestamp.from(request.runAt()),
                    request.runAt() == null ? null : java.sql.Timestamp.from(request.runAt()), idempotencyKey);
            if (inserted.isEmpty()) {
                List<Job> found = jdbc.query("SELECT * FROM jobs WHERE idempotency_key=?", this::mapJob, idempotencyKey);
                if (found.isEmpty()) throw new IllegalStateException("Idempotency key conflict without row");
                Job previous = found.getFirst();
                if (!previous.type().equals(request.type()) || !previous.payload().equals(request.payload()) ||
                    previous.priority() != request.effectivePriority() ||
                    previous.maxRetries() != request.effectiveMaxRetries() ||
                    previous.timeoutSeconds() != request.effectiveTimeoutSeconds() ||
                    !Objects.equals(previous.scheduledFor(), request.runAt()))
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency-Key reused with different request");
                return previous;
            }
            return getJob(id);
        }));
    }

    public Optional<ClaimedJob> claim(String workerId, int leaseGraceSeconds) {
        return Objects.requireNonNull(tx.execute(status -> {
            Boolean draining = jdbc.queryForObject("SELECT draining FROM queue_control WHERE id=1 FOR SHARE", Boolean.class);
            if (Boolean.TRUE.equals(draining)) return Optional.empty();
            List<Job> due = jdbc.query("""
                    SELECT * FROM jobs
                    WHERE status IN ('QUEUED','FAILED') AND available_at <= now()
                    ORDER BY priority DESC, available_at ASC, created_at ASC
                    LIMIT 1 FOR UPDATE SKIP LOCKED
                    """, this::mapJob);
            if (due.isEmpty()) return Optional.empty();
            Job job = due.getFirst();
            UUID token = UUID.randomUUID();
            UUID executionId = UUID.randomUUID();
            int attempt = job.attemptCount() + 1;
            jdbc.update("""
                    UPDATE jobs SET status='RUNNING', attempt_count=?, lease_token=?, current_execution_id=?,
                      lease_until=now() + (? * interval '1 second'), updated_at=now()
                    WHERE id=?
                    """, attempt, token, executionId, job.timeoutSeconds() + leaseGraceSeconds, job.id());
            jdbc.update("""
                    INSERT INTO job_executions(id,job_id,attempt_number,worker_id,lease_token,status)
                    VALUES(?,?,?,?,?,'RUNNING')
                    """, executionId, job.id(), attempt, workerId, token);
            return Optional.of(new ClaimedJob(job.id(), executionId, token, job.type(),
                    job.payload(), attempt, job.maxRetries(), job.timeoutSeconds()));
        }));
    }

    // Serialize completion with recovery via the parent job's row lock.
    public boolean complete(ClaimedJob job, JsonNode result) {
        boolean updated = Boolean.TRUE.equals(tx.execute(status -> {
            if (!owns(job)) return false;
            jdbc.update("""
                    UPDATE job_executions SET status='SUCCEEDED', result=?::jsonb, finished_at=now()
                    WHERE id=? AND status='RUNNING'
                    """, stringify(result), job.executionId());
            jdbc.update("""
                    UPDATE jobs SET status='SUCCEEDED', result=?::jsonb, last_error=NULL,
                      lease_token=NULL, lease_until=NULL, current_execution_id=NULL, updated_at=now()
                    WHERE id=?
                    """, stringify(result), job.id());
            return true;
        }));
        if (updated) metrics.counter("jobs.completed.total", "outcome", "succeeded").increment();
        return updated;
    }

    public boolean fail(ClaimedJob job, String failureStatus, String error) {
        if (!List.of("FAILED", "TIMED_OUT").contains(failureStatus)) throw new IllegalArgumentException("Invalid failure state");
        return failOwned(job.id(), job.executionId(), job.leaseToken(), failureStatus, error);
    }

    private boolean failOwned(UUID id, UUID executionId, UUID token, String failureStatus, String error) {
        String cleanError = error == null ? "Unknown error" : error.substring(0, Math.min(error.length(), 2000));
        String outcome = tx.execute(status -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT id, attempt_count, max_retries, lease_token, current_execution_id FROM jobs WHERE id=? FOR UPDATE", id);
            if (rows.isEmpty() || !token.equals(rows.getFirst().get("lease_token")) ||
                !executionId.equals(rows.getFirst().get("current_execution_id"))) return null;
            int attempts = ((Number) rows.getFirst().get("attempt_count")).intValue();
            int retries = ((Number) rows.getFirst().get("max_retries")).intValue();
            jdbc.update("""
                    UPDATE job_executions SET status=?, error_message=?, finished_at=now()
                    WHERE id=? AND status='RUNNING'
                    """, failureStatus, cleanError, executionId);
            if (RetryPolicy.canRetry(attempts, retries)) {
                long delay = RetryPolicy.delayAfterFailure(attempts).toSeconds();
                jdbc.update("""
                        UPDATE jobs SET status='FAILED', last_error=?, available_at=now() + (? * interval '1 second'),
                        lease_token=NULL, lease_until=NULL, current_execution_id=NULL, updated_at=now() WHERE id=?
                        """, cleanError, delay, id);
                return "retry";
            }
            jdbc.update("""
                    UPDATE jobs SET status='DEAD_LETTERED', last_error=?, lease_token=NULL,
                    lease_until=NULL, current_execution_id=NULL, updated_at=now() WHERE id=?
                    """, cleanError, id);
            jdbc.update("""
                    INSERT INTO dead_letters(job_id,last_error,attempt_count) VALUES(?,?,?)
                    ON CONFLICT (job_id) DO NOTHING
                    """, id, cleanError, attempts);
            return "dead_lettered";
        });
        if (outcome == null) return false;
        metrics.counter("jobs.completed.total", "outcome", failureStatus.toLowerCase()).increment();
        if (outcome.equals("retry")) metrics.counter("jobs.retries.total").increment();
        else metrics.counter("jobs.dead_letter.total").increment();
        return true;
    }

    private boolean owns(ClaimedJob job) {
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT lease_token,current_execution_id,status FROM jobs WHERE id=? FOR UPDATE", job.id());
        return !rows.isEmpty() && "RUNNING".equals(rows.getFirst().get("status")) &&
                job.leaseToken().equals(rows.getFirst().get("lease_token")) &&
                job.executionId().equals(rows.getFirst().get("current_execution_id"));
    }

    public int recoverExpired(int limit) {
        List<UUID> expired = Objects.requireNonNull(tx.execute(status -> jdbc.query("""
                SELECT id FROM jobs WHERE status='RUNNING' AND lease_until < now()
                ORDER BY lease_until LIMIT ? FOR UPDATE SKIP LOCKED
                """, (rs, row) -> (UUID) rs.getObject(1), limit)));
        // Separate short transactions avoid holding locks while updating many independent jobs.
        int recovered = 0;
        for (UUID id : expired) {
            Boolean done = tx.execute(status -> {
                List<Map<String, Object>> rows = jdbc.queryForList("""
                        SELECT current_execution_id, lease_token FROM jobs
                        WHERE id=? AND status='RUNNING' AND lease_until < now() FOR UPDATE
                        """, id);
                if (rows.isEmpty()) return false;
                Map<String,Object> row = rows.getFirst();
                UUID exec = (UUID) row.get("current_execution_id");
                UUID token = (UUID) row.get("lease_token");
                // failOwned runs with REQUIRED propagation (TransactionTemplate's default).
                return failOwned(id, exec, token, "LOST", "Worker lease expired; outcome unknown");
            });
            if (Boolean.TRUE.equals(done)) recovered++;
        }
        return recovered;
    }

    public List<Map<String,Object>> executions(UUID id) {
        getJob(id);
        return jdbc.query("""
                SELECT id, job_id, attempt_number, worker_id, status, started_at, finished_at, error_message, result
                FROM job_executions WHERE job_id=? ORDER BY attempt_number
                """, (rs, n) -> {
            Map<String,Object> row = new LinkedHashMap<>();
            row.put("executionId", rs.getObject("id"));
            row.put("attempt", rs.getInt("attempt_number"));
            row.put("workerId", rs.getString("worker_id"));
            row.put("status", rs.getString("status"));
            row.put("startedAt", instant(rs, "started_at"));
            row.put("finishedAt", instant(rs, "finished_at"));
            row.put("error", rs.getString("error_message"));
            row.put("result", parse(rs.getString("result")));
            return row;
        }, id);
    }

    public Map<String,Object> depth() {
        Map<String,Object> values = jdbc.queryForMap("""
                SELECT count(*) FILTER (WHERE status='QUEUED' AND available_at <= now()) AS ready,
                       count(*) FILTER (WHERE status='QUEUED' AND available_at > now()) AS delayed,
                       count(*) FILTER (WHERE status='FAILED' AND available_at <= now()) AS retry_ready,
                       count(*) FILTER (WHERE status='FAILED' AND available_at > now()) AS retry_wait,
                       count(*) FILTER (WHERE status='RUNNING') AS running,
                       count(*) FILTER (WHERE status='SUCCEEDED') AS succeeded,
                       count(*) FILTER (WHERE status='DEAD_LETTERED') AS dead_lettered,
                       count(*) FILTER (WHERE status='CANCELLED') AS cancelled
                FROM jobs
                """);
        Map<String,Object> out = new LinkedHashMap<>(values);
        out.put("draining", isDraining());
        return out;
    }

    public double readyCount() {
        Long count = jdbc.queryForObject("""
                SELECT count(*) FROM jobs WHERE status IN ('QUEUED','FAILED') AND available_at <= now()
                """, Long.class);
        return count == null ? 0 : count;
    }

    public boolean isDraining() { return Boolean.TRUE.equals(jdbc.queryForObject("SELECT draining FROM queue_control WHERE id=1", Boolean.class)); }

    public void drain(boolean draining) {
        jdbc.update("UPDATE queue_control SET draining=?,updated_at=now() WHERE id=1", draining);
    }

    public int cancelPending() {
        return jdbc.update("""
                UPDATE jobs SET status='CANCELLED', updated_at=now()
                WHERE status IN ('QUEUED','FAILED')
                """);
    }

    public boolean cancel(UUID id) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            List<String> states = jdbc.query("SELECT status FROM jobs WHERE id=? FOR UPDATE", (rs,n) -> rs.getString(1), id);
            if (states.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found");
            if (!List.of("QUEUED", "FAILED").contains(states.getFirst()))
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Only pending jobs can be cancelled");
            return jdbc.update("UPDATE jobs SET status='CANCELLED',updated_at=now() WHERE id=?", id) == 1;
        }));
    }
}
