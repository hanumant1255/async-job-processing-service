package com.example.jobservice.api;

import com.example.jobservice.repository.JobRepository;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/queue")
@ConditionalOnProperty(name="jobs.api.enabled", havingValue="true", matchIfMissing=true)
public class QueueController {
    private final JobRepository jobs;
    public QueueController(JobRepository jobs) { this.jobs = jobs; }

    @GetMapping("/depth")
    public Map<String,Object> depth() { return jobs.depth(); }

    @PostMapping("/drain")
    public Map<String,Object> drain() {
        jobs.drain(true);
        return Map.of("draining", true, "message", "New claims paused; running attempts continue");
    }

    @PostMapping("/resume")
    public Map<String,Object> resume() {
        jobs.drain(false);
        return Map.of("draining", false);
    }

    @PostMapping("/cancel-pending")
    public Map<String,Object> cancelPending() {
        return Map.of("cancelledCount", jobs.cancelPending());
    }
}
