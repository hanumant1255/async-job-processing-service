package com.example.jobservice.domain;

import java.time.Duration;

public final class RetryPolicy {
    private RetryPolicy() {}

    // Capped exponential backoff; the first retry waits two seconds.
    public static Duration delayAfterFailure(int completedAttempt) {
        if (completedAttempt < 1) throw new IllegalArgumentException("attempt must be >= 1");
        long seconds = Math.min(60, 1L << Math.min(completedAttempt, 6));
        return Duration.ofSeconds(seconds);
    }

    public static boolean canRetry(int completedAttempts, int maxRetries) {
        return completedAttempts <= maxRetries;
    }
}
