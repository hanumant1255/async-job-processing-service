package com.example.jobservice.api;

import com.example.jobservice.domain.*;
import com.example.jobservice.repository.JobRepository;
import jakarta.validation.Valid;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@ConditionalOnProperty(name="jobs.api.enabled", havingValue="true", matchIfMissing=true)
public class JobController {
    private final JobRepository repository;
    private final Handlers handlers;
    public JobController(JobRepository repository, Handlers handlers) {
        this.repository = repository;
        this.handlers = handlers;
    }

    @PostMapping("/jobs")
    public ResponseEntity<Job> submit(@Valid @RequestBody JobRequest request) {
        requireSupported(request.type());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(repository.submit(request));
    }

    @GetMapping("/jobs/{id}")
    public Job get(@PathVariable UUID id) { return repository.getJob(id); }

    @GetMapping("/jobs/{id}/executions")
    public List<Map<String,Object>> executions(@PathVariable UUID id) { return repository.executions(id); }

    @PostMapping("/jobs/{id}/cancel")
    public Map<String,Object> cancel(@PathVariable UUID id) {
        return Map.of("jobId", id, "cancelled", repository.cancel(id));
    }

    @GetMapping("/handlers")
    public Set<String> handlers() { return handlers.supportedTypes(); }

    void requireSupported(String type) {
        if (!handlers.supportedTypes().contains(type))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported handler type: " + type);
    }
}
