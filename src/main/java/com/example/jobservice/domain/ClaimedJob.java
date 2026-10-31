package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

public record ClaimedJob(UUID id, UUID executionId, UUID leaseToken, String type,
                         JsonNode payload, int attemptNumber, int maxRetries, int timeoutSeconds) {}
