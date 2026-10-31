package com.example.jobservice.domain;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {
    @Test void exponentialDelayIsCapped() {
        assertEquals(Duration.ofSeconds(2), RetryPolicy.delayAfterFailure(1));
        assertEquals(Duration.ofSeconds(4), RetryPolicy.delayAfterFailure(2));
        assertEquals(Duration.ofSeconds(8), RetryPolicy.delayAfterFailure(3));
        assertEquals(Duration.ofSeconds(60), RetryPolicy.delayAfterFailure(50));
    }
    @Test void maxRetriesMeansAdditionalAttempts() {
        assertTrue(RetryPolicy.canRetry(1, 3));
        assertTrue(RetryPolicy.canRetry(3, 3));
        assertFalse(RetryPolicy.canRetry(4, 3));
        assertFalse(RetryPolicy.canRetry(1, 0));
    }
    @Test void invalidAttemptRejected() {
        assertThrows(IllegalArgumentException.class, () -> RetryPolicy.delayAfterFailure(0));
    }
}
