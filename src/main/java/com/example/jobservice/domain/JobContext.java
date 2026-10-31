package com.example.jobservice.domain;

import java.util.UUID;

/** Stable logical-job identity and the identity of this particular attempt. */
public record JobContext(UUID jobId, UUID executionId, int attemptNumber) {}
