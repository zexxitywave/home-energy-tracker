package com.leetjourney.api_gateway.route;

import org.springframework.cloud.gateway.server.mvc.filter.CircuitBreakerFilterFunctions;
import org.springframework.cloud.gateway.server.mvc.handler.GatewayRouterFunctions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.RequestPredicates;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import java.net.URI;

import static org.springframework.cloud.gateway.server.mvc.filter.BeforeFilterFunctions.uri;
import static org.springframework.cloud.gateway.server.mvc.filter.FilterFunctions.setPath;
import static org.springframework.cloud.gateway.server.mvc.handler.GatewayRouterFunctions.route;
import static org.springframework.cloud.gateway.server.mvc.handler.HandlerFunctions.http;

@Configuration
public class IngestionServiceRoutes {

    @Bean
    public RouterFunction<ServerResponse> ingestionRoute() {
        return route("ingestion-service")
                .route(RequestPredicates.path("/api/v1/ingestion/**"), http())
                .before(uri("http://ingestion-service:8082"))
                .filter(CircuitBreakerFilterFunctions.circuitBreaker(
                        "ingestionServiceCircuitBreaker",
                        URI.create("forward:/ingestionFallbackRoute")
                ))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> ingestionFallbackRoute() {
        return route("ingestionFallbackRoute")
                .route(RequestPredicates.path("/ingestionFallbackRoute"),
                        request -> ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body("Ingestion service is down"))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> ingestionServiceApiDocs() {
        return GatewayRouterFunctions.route("ingestion-service-api-docs")
                .route(RequestPredicates.path("/docs/ingestion-service/v3/api-docs"), http())
                .before(uri("http://ingestion-service:8082"))
                .filter(setPath("/v3/api-docs"))
                .build();
    }
}