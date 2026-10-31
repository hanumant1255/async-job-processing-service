package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record Job(
        UUID id,
        UUID scheduleId,
        Instant scheduledFor,
        String type,
        JsonNode payload,
        String status,
        int priority,
        int attemptCount,
        int maxRetries,
        int timeoutSeconds,
        Instant availableAt,
        JsonNode result,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {}
