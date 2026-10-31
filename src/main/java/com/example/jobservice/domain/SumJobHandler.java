package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class SumJobHandler implements JobHandler {
    private final ObjectMapper mapper;
    public SumJobHandler(ObjectMapper mapper) { this.mapper = mapper; }
    @Override public String type() { return "sum"; }
    @Override public JsonNode execute(JsonNode payload, int attemptNumber) {
        JsonNode values = payload.path("numbers");
        if (!values.isArray()) throw new IllegalArgumentException("numbers must be an array");
        long sum = 0;
        for (JsonNode value : values) {
            if (!value.isIntegralNumber() || !value.canConvertToLong())
                throw new IllegalArgumentException("all numbers must be 64-bit integers");
            sum = Math.addExact(sum, value.longValue());
        }
        ObjectNode result = mapper.createObjectNode();
        result.put("sum", sum);
        return result;
    }
}
