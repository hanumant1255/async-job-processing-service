package com.example.jobservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.springframework.stereotype.Component;

@Component
public class Handlers {
    private final Map<String, JobHandler> registry;

    public Handlers(List<JobHandler> handlers) {
        Map<String,JobHandler> entries = new HashMap<>();
        for (JobHandler handler : handlers) {
            if (entries.putIfAbsent(handler.type(), handler) != null)
                throw new IllegalArgumentException("Duplicate handler type: " + handler.type());
        }
        this.registry = Map.copyOf(entries);
    }

    public Set<String> supportedTypes() { return registry.keySet(); }

    public JsonNode run(String type, JsonNode payload, JobContext context) throws Exception {
        JobHandler handler = registry.get(type);
        if (handler == null) throw new IllegalArgumentException("Unsupported handler: " + type);
        return handler.execute(payload, context);
    }
}
