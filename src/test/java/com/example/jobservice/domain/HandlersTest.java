package com.example.jobservice.domain;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HandlersTest {
    ObjectMapper mapper = new ObjectMapper();
    Handlers handlers;
    @BeforeEach void init() { handlers = new Handlers(java.util.List.of(new SumJobHandler(mapper),
                new SleepJobHandler(mapper), new FlakyJobHandler(mapper))); }

    @Test void sumProducesResult() throws Exception {
        var result = handlers.run("sum", mapper.readTree("{\"numbers\":[1,2,3,4]}"), 1);
        assertEquals(10, result.get("sum").asInt());
    }
    @Test void flakyJobEventuallySucceeds() throws Exception {
        var payload = mapper.readTree("{\"failUntilAttempt\":2}");
        assertThrows(IllegalStateException.class, () -> handlers.run("flaky", payload, 1));
        assertThrows(IllegalStateException.class, () -> handlers.run("flaky", payload, 2));
        assertEquals(3, handlers.run("flaky", payload, 3).get("completedOnAttempt").asInt());
    }
    @Test void rejectsUnknownHandlerAndInvalidInput() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> handlers.run("unknown", mapper.createObjectNode(), 1));
        assertThrows(IllegalArgumentException.class, () -> handlers.run("sum", mapper.readTree("{\"numbers\":[\"bad\"]}"), 1));
    }
    @Test void sleepCanBeInterrupted() throws Exception {
        Thread worker = new Thread(() -> {
            try { handlers.run("sleep", mapper.readTree("{\"millis\":300000}"), 1); }
            catch (InterruptedException expected) { return; }
            catch (Exception e) { throw new RuntimeException(e); }
            throw new AssertionError("Expected interruption");
        });
        worker.start();
        worker.interrupt();
        worker.join(2000);
        assertFalse(worker.isAlive());
    }
}
