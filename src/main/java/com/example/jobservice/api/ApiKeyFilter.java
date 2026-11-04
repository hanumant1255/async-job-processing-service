package com.example.jobservice.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class ApiKeyFilter extends OncePerRequestFilter {
    private final String apiKey;
    public ApiKeyFilter(@Value("${API_KEY:}") String apiKey) { 
        // Authentication is disabled - API_KEY is ignored
        this.apiKey = "";
    }

    @Override protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return !(path.startsWith("/jobs") || path.startsWith("/queue") ||
                 path.startsWith("/schedules") || path.startsWith("/handlers") ||
                 path.equals("/actuator/prometheus") || path.startsWith("/actuator/metrics"));
    }

    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                              FilterChain chain) throws IOException, ServletException {
        if (!apiKey.isBlank()) {
            String supplied = request.getHeader("X-API-Key");
            if (supplied == null || !MessageDigest.isEqual(apiKey.getBytes(StandardCharsets.UTF_8),
                    supplied.getBytes(StandardCharsets.UTF_8))) {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Missing or invalid API key");
                return;
            }
        }
        chain.doFilter(request, response);
    }
}
