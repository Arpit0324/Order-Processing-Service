package com.ops.gateway.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.http.server.reactive.MockServerHttpResponse;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ResponseStatusException;
import reactor.test.StepVerifier;

import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalErrorWebExceptionHandlerTest {

    private GlobalErrorWebExceptionHandler handler;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        handler = new GlobalErrorWebExceptionHandler(objectMapper);
    }

    @Test
    void handle_connectException_shouldReturn502() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders")
                        .header("X-Trace-Id", "trace-123")
                        .build());

        StepVerifier.create(handler.handle(exchange, new ConnectException("Connection refused")))
                .verifyComplete();

        MockServerHttpResponse response = exchange.getResponse();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
    }

    @Test
    void handle_timeoutException_shouldReturn504() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders")
                        .header("X-Trace-Id", "trace-456")
                        .build());

        StepVerifier.create(handler.handle(exchange, new TimeoutException("Read timed out")))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
    }

    @Test
    void handle_responseStatusException_shouldMapStatus() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").build());

        ResponseStatusException ex = new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid input");

        StepVerifier.create(handler.handle(exchange, ex))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void handle_genericException_shouldReturn500() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders")
                        .header("X-Trace-Id", "trace-789")
                        .build());

        StepVerifier.create(handler.handle(exchange, new RuntimeException("Unexpected")))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void handle_wrappedConnectException_shouldReturn502() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").build());

        Exception wrapped = new RuntimeException("IO error", new ConnectException("Connection refused"));

        StepVerifier.create(handler.handle(exchange, wrapped))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
    }

    @Test
    void handle_shouldIncludeTraceIdInResponse() throws Exception {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders")
                        .header("X-Trace-Id", "my-trace-id")
                        .build());

        StepVerifier.create(handler.handle(exchange, new ConnectException("fail")))
                .verifyComplete();

        String body = exchange.getResponse().getBodyAsString().block();
        ErrorResponse errorResponse = objectMapper.readValue(body, ErrorResponse.class);
        assertThat(errorResponse.traceId()).isEqualTo("my-trace-id");
        assertThat(errorResponse.error()).isEqualTo("BAD_GATEWAY");
        assertThat(errorResponse.status()).isEqualTo(502);
        assertThat(errorResponse.timestamp()).isNotNull();
    }

    @Test
    void handle_withoutTraceId_shouldReturnNullTraceId() throws Exception {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").build());

        StepVerifier.create(handler.handle(exchange, new TimeoutException("timeout")))
                .verifyComplete();

        String body = exchange.getResponse().getBodyAsString().block();
        ErrorResponse errorResponse = objectMapper.readValue(body, ErrorResponse.class);
        assertThat(errorResponse.traceId()).isNull();
        assertThat(errorResponse.error()).isEqualTo("GATEWAY_TIMEOUT");
        assertThat(errorResponse.status()).isEqualTo(504);
    }
}
