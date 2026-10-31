package com.example.jobservice.repository;

import com.example.jobservice.domain.ScheduleRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class ScheduleRepository {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final TransactionTemplate tx;

    public ScheduleRepository(JdbcTemplate jdbc, ObjectMapper json,
                              org.springframework.transaction.PlatformTransactionManager tm) {
        this.jdbc = jdbc;
        this.json = json;
        this.tx = new TransactionTemplate(tm);
    }

    public record Schedule(UUID id, String type, String payload, String cronExpression, String zoneId,
                           boolean enabled, int priority, int maxRetries, int timeoutSeconds, Instant nextRunAt) {}

    private Schedule map(ResultSet rs, int row) throws SQLException {
        return new Schedule((UUID)rs.getObject("id"), rs.getString("type"), rs.getString("payload"),
                rs.getString("cron_expression"), rs.getString("zone_id"), rs.getBoolean("enabled"),
                rs.getInt("priority"), rs.getInt("max_retries"), rs.getInt("timeout_seconds"),
                rs.getTimestamp("next_run_at").toInstant());
    }

    private static Instant next(String cron, String timezone, Instant after) {
        ZonedDateTime next = CronExpression.parse(cron).next(after.atZone(ZoneId.of(timezone)));
        if (next == null) throw new IllegalArgumentException("Cron expression has no next occurrence");
        return next.toInstant();
    }

    public Schedule create(ScheduleRequest request) {
        Instant due;
        try { due = next(request.cronExpression(), request.timezone(), Instant.now()); }
        catch (java.time.DateTimeException | IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid cron expression or timezone: " + e.getMessage());
        }
        UUID id = UUID.randomUUID();
        String payload;
        try { payload = json.writeValueAsString(request.payload()); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("Invalid JSON", e); }
        jdbc.update("""
                INSERT INTO job_schedules(id,type,payload,cron_expression,zone_id,priority,max_retries,timeout_seconds,next_run_at)
                VALUES(?,?,?::jsonb,?,?,?,?,?,?)
                """, id, request.type(), payload, request.cronExpression(), request.timezone(),
                request.effectivePriority(), request.effectiveMaxRetries(), request.effectiveTimeoutSeconds(),
                java.sql.Timestamp.from(due));
        return get(id);
    }

    public Schedule get(UUID id) {
        List<Schedule> found = jdbc.query("SELECT * FROM job_schedules WHERE id=?", this::map, id);
        if (found.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Schedule not found");
        return found.getFirst();
    }

    public void disable(UUID id) {
        tx.executeWithoutResult(status -> {
            if (jdbc.update("UPDATE job_schedules SET enabled=false,updated_at=now() WHERE id=?", id) == 0)
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Schedule not found");
            // Cancel unclaimed occurrences atomically with disabling the schedule.
            jdbc.update("UPDATE jobs SET status='CANCELLED',updated_at=now() WHERE schedule_id=? AND status IN ('QUEUED','FAILED')", id);
        });
    }

    public int materializeDue(int batchSize) {
        return Objects.requireNonNull(tx.execute(status -> {
            List<Schedule> due = jdbc.query("""
                    SELECT * FROM job_schedules WHERE enabled AND next_run_at <= now()
                    ORDER BY next_run_at,id LIMIT ? FOR UPDATE SKIP LOCKED
                    """, this::map, batchSize);
            for (Schedule schedule : due) {
                Instant occurrence = schedule.nextRunAt();
                UUID jobId = UUID.randomUUID();
                jdbc.update("""
                        INSERT INTO jobs(id,schedule_id,scheduled_for,type,payload,status,priority,max_retries,timeout_seconds,available_at)
                        VALUES(?,?,?, ?,?::jsonb,'QUEUED',?,?,?,?)
                        ON CONFLICT (schedule_id,scheduled_for) DO NOTHING
                        """, jobId, schedule.id(), java.sql.Timestamp.from(occurrence), schedule.type(),
                        schedule.payload(), schedule.priority(), schedule.maxRetries(), schedule.timeoutSeconds(),
                        java.sql.Timestamp.from(occurrence));
                Instant upcoming = next(schedule.cronExpression(), schedule.zoneId(), occurrence);
                jdbc.update("UPDATE job_schedules SET next_run_at=?,updated_at=now() WHERE id=?",
                        java.sql.Timestamp.from(upcoming), schedule.id());
            }
            return due.size();
        }));
    }
}
