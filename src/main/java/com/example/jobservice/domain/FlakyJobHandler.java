package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class FlakyJobHandler implements JobHandler {
    private final ObjectMapper mapper;
    public FlakyJobHandler(ObjectMapper mapper) { this.mapper = mapper; }
    @Override public String type() { return "flaky"; }
    @Override public JsonNode execute(JsonNode payload, int attemptNumber) {
        int failUntil = payload.path("failUntilAttempt").asInt(1);
        if (failUntil < 0 || failUntil > 100) throw new IllegalArgumentException("invalid failUntilAttempt");
        if (attemptNumber <= failUntil) throw new IllegalStateException("Simulated transient failure on attempt " + attemptNumber);
        ObjectNode result = mapper.createObjectNode();
        result.put("completedOnAttempt", attemptNumber);
        return result;
    }
}
