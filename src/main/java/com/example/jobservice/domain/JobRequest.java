package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.*;
import java.time.Instant;

public record JobRequest(
        @NotBlank String type,
        @NotNull JsonNode payload,
        @Min(-100) @Max(100) Integer priority,
        @Min(0) @Max(100) Integer maxRetries,
        @Min(1) @Max(86400) Integer timeoutSeconds,
        Instant runAt
) {
    public int effectivePriority() { return priority == null ? 0 : priority; }
    public int effectiveMaxRetries() { return maxRetries == null ? 3 : maxRetries; }
    public int effectiveTimeoutSeconds() { return timeoutSeconds == null ? 30 : timeoutSeconds; }
}
