package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class SleepJobHandler implements JobHandler {
    private final ObjectMapper mapper;
    public SleepJobHandler(ObjectMapper mapper) { this.mapper = mapper; }
    @Override public String type() { return "sleep"; }
    @Override public JsonNode execute(JsonNode payload, int attemptNumber) throws InterruptedException {
        long millis = payload.path("millis").asLong(1000);
        if (millis < 0 || millis > 300_000) throw new IllegalArgumentException("millis must be between 0 and 300000");
        Thread.sleep(millis);
        ObjectNode result = mapper.createObjectNode();
        result.put("sleptMillis", millis);
        return result;
    }
}
