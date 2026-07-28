package com.ops.gateway.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.gateway.exception.ErrorResponse;
import com.ops.gateway.exception.GatewayErrorCode;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// ── Post-filter: rewrites raw (non-JSON) 5xx responses into standard envelope ─
// If a downstream service returns a 5xx with a non-JSON body (e.g., HTML error page,
// plain text, or empty body), this filter replaces it with the standard ErrorResponse.
// Well-formed JSON responses from downstream pass through untouched.
@Component
public class ErrorResponseFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(ErrorResponseFilter.class);

    private final ObjectMapper objectMapper;

    public ErrorResponseFilter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public int getOrder() {
        // Run very early so the decorator wraps the response before other filters write to it
        return -2;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpResponse originalResponse = exchange.getResponse();
        ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(originalResponse) {

            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                var rawStatus = getDelegate().getStatusCode();
                HttpStatus status = HttpStatus.resolve(
                        rawStatus != null ? rawStatus.value() : 200);

                // Only intercept 5xx responses
                if (status == null || !status.is5xxServerError()) {
                    return super.writeWith(body);
                }

                // If downstream already returns JSON, pass through
                MediaType contentType = getDelegate().getHeaders().getContentType();
                if (contentType != null && contentType.isCompatibleWith(MediaType.APPLICATION_JSON)) {
                    return super.writeWith(body);
                }

                // Rewrite raw 5xx → standard envelope
                String traceId = exchange.getRequest().getHeaders().getFirst("X-Trace-Id");
                GatewayErrorCode errorCode = mapStatus(status);

                log.warn("Rewriting raw {} response to standard envelope traceId={}", status.value(), traceId);

                ErrorResponse errorResponse = ErrorResponse.of(errorCode, traceId);

                // Release the original body buffers
                return DataBufferUtils.join(Flux.from(body))
                        .flatMap(dataBuffer -> {
                            DataBufferUtils.release(dataBuffer);
                            return writeErrorResponse(getDelegate(), errorResponse);
                        })
                        .switchIfEmpty(writeErrorResponse(getDelegate(), errorResponse));
            }

            @Override
            public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
                return writeWith(Flux.from(body).flatMapSequential(Flux::from));
            }
        };

        return chain.filter(exchange.mutate().response(decoratedResponse).build());
    }

    private Mono<Void> writeErrorResponse(ServerHttpResponse response, ErrorResponse errorResponse) {
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(errorResponse);
            response.getHeaders().setContentLength(bytes.length);
            var buffer = response.bufferFactory().wrap(bytes);
            return response.writeWith(Mono.just(buffer));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize error response", e);
            byte[] fallback = """
                    {"error":"INTERNAL_ERROR","message":"An unexpected error occurred","status":500}
                    """.getBytes();
            var buffer = response.bufferFactory().wrap(fallback);
            return response.writeWith(Mono.just(buffer));
        }
    }

    private GatewayErrorCode mapStatus(HttpStatus status) {
        return switch (status) {
            case BAD_GATEWAY -> GatewayErrorCode.BAD_GATEWAY;
            case GATEWAY_TIMEOUT -> GatewayErrorCode.GATEWAY_TIMEOUT;
            case SERVICE_UNAVAILABLE -> GatewayErrorCode.SERVICE_UNAVAILABLE;
            default -> GatewayErrorCode.INTERNAL_ERROR;
        };
    }
}
