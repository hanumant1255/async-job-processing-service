package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;

public interface JobHandler {
    String type();
    JsonNode execute(JsonNode payload, int attemptNumber) throws Exception;
}
