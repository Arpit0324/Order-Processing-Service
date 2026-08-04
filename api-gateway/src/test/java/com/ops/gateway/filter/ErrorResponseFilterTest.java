package com.ops.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.gateway.exception.ErrorResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.http.server.reactive.MockServerHttpResponse;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ErrorResponseFilterTest {

    private ErrorResponseFilter filter;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        filter = new ErrorResponseFilter(objectMapper);
    }

    @Test
    void filter_orderShouldBeNegative2() {
        assertThat(filter.getOrder()).isEqualTo(-2);
    }

    @Test
    void filter_2xxResponse_shouldPassThrough() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").build());

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(any(ServerWebExchange.class))).thenAnswer(invocation -> {
            ServerWebExchange ex = invocation.getArgument(0);
            ex.getResponse().setStatusCode(HttpStatus.OK);
            ex.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
            DataBuffer buffer = new DefaultDataBufferFactory()
                    .wrap("{\"id\":1}".getBytes(StandardCharsets.UTF_8));
            return ex.getResponse().writeWith(Mono.just(buffer));
        });

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void filter_5xxWithJsonResponse_shouldPassThrough() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").build());

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(any(ServerWebExchange.class))).thenAnswer(invocation -> {
            ServerWebExchange ex = invocation.getArgument(0);
            ex.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
            ex.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
            String jsonBody = "{\"error\":\"VALIDATION_FAILED\",\"message\":\"Downstream error\"}";
            DataBuffer buffer = new DefaultDataBufferFactory()
                    .wrap(jsonBody.getBytes(StandardCharsets.UTF_8));
            return ex.getResponse().writeWith(Mono.just(buffer));
        });

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        // JSON 5xx passes through untouched
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void filter_5xxWithHtmlResponse_shouldRewriteToJsonEnvelope() throws Exception {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders")
                        .header("X-Trace-Id", "trace-rewrite")
                        .build());

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(any(ServerWebExchange.class))).thenAnswer(invocation -> {
            ServerWebExchange ex = invocation.getArgument(0);
            ex.getResponse().setStatusCode(HttpStatus.BAD_GATEWAY);
            ex.getResponse().getHeaders().setContentType(MediaType.TEXT_HTML);
            DataBuffer buffer = new DefaultDataBufferFactory()
                    .wrap("<html><body>502 Bad Gateway</body></html>".getBytes(StandardCharsets.UTF_8));
            return ex.getResponse().writeWith(Mono.just(buffer));
        });

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        MockServerHttpResponse response = exchange.getResponse();
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);

        String body = response.getBodyAsString().block();
        ErrorResponse errorResponse = objectMapper.readValue(body, ErrorResponse.class);
        assertThat(errorResponse.error()).isEqualTo("BAD_GATEWAY");
        assertThat(errorResponse.status()).isEqualTo(502);
        assertThat(errorResponse.traceId()).isEqualTo("trace-rewrite");
    }

    @Test
    void filter_503WithNoContentType_shouldRewriteToJsonEnvelope() throws Exception {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/inventory").build());

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(any(ServerWebExchange.class))).thenAnswer(invocation -> {
            ServerWebExchange ex = invocation.getArgument(0);
            ex.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
            // No content type set (raw response)
            DataBuffer buffer = new DefaultDataBufferFactory()
                    .wrap("Service Unavailable".getBytes(StandardCharsets.UTF_8));
            return ex.getResponse().writeWith(Mono.just(buffer));
        });

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        String body = exchange.getResponse().getBodyAsString().block();
        ErrorResponse errorResponse = objectMapper.readValue(body, ErrorResponse.class);
        assertThat(errorResponse.error()).isEqualTo("SERVICE_UNAVAILABLE");
        assertThat(errorResponse.status()).isEqualTo(503);
    }
}
