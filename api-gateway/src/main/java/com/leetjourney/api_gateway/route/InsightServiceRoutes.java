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
public class InsightServiceRoutes {

    @Bean
    public RouterFunction<ServerResponse> insightRoute() {
        return route("insight-service")
                .route(RequestPredicates.path("/api/v1/insight/**"), http())
                .before(uri("http://insight-service:8085"))
                .filter(CircuitBreakerFilterFunctions.circuitBreaker(
                        "insightServiceCircuitBreaker",
                        URI.create("forward:/insightFallbackRoute")
                ))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> insightFallbackRoute() {
        return route("insightFallbackRoute")
                .route(RequestPredicates.path("/insightFallbackRoute"),
                        request -> ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body("Insight service is down"))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> insightServiceApiDocs() {
        return GatewayRouterFunctions.route("insight-service-api-docs")
                .route(RequestPredicates.path("/docs/insight-service/v3/api-docs"), http())
                .before(uri("http://insight-service:8085"))
                .filter(setPath("/v3/api-docs"))
                .build();
    }
}