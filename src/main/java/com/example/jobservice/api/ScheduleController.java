package com.example.jobservice.api;

import com.example.jobservice.domain.ScheduleRequest;
import com.example.jobservice.repository.ScheduleRepository;
import jakarta.validation.Valid;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/schedules")
@ConditionalOnProperty(name="jobs.api.enabled", havingValue="true", matchIfMissing=true)
public class ScheduleController {
    private final ScheduleRepository schedules;
    private final JobController jobs;
    public ScheduleController(ScheduleRepository schedules, JobController jobs) {
        this.schedules = schedules;
        this.jobs = jobs;
    }
    @PostMapping
    public ResponseEntity<ScheduleRepository.Schedule> create(@Valid @RequestBody ScheduleRequest request) {
        jobs.requireSupported(request.type());
        return ResponseEntity.status(HttpStatus.CREATED).body(schedules.create(request));
    }
    @GetMapping("/{id}")
    public ScheduleRepository.Schedule get(@PathVariable UUID id) { return schedules.get(id); }
    @DeleteMapping("/{id}")
    public Map<String,Object> disable(@PathVariable UUID id) {
        schedules.disable(id);
        return Map.of("scheduleId", id, "enabled", false);
    }
}
