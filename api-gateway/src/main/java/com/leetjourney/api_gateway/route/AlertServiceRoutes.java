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
public class AlertServiceRoutes {

    @Bean
    public RouterFunction<ServerResponse> alertRoute() {
        return route("alert-service")
                .route(RequestPredicates.path("/api/v1/alert/**"), http())
                .before(uri("http://alert-service:8084"))
                .filter(CircuitBreakerFilterFunctions.circuitBreaker(
                        "alertServiceCircuitBreaker",
                        URI.create("forward:/alertFallbackRoute")
                ))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> alertFallbackRoute() {
        return route("alertFallbackRoute")
                .route(RequestPredicates.path("/alertFallbackRoute"),
                        request -> ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body("Alert service is down"))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> alertServiceApiDocs() {
        return GatewayRouterFunctions.route("alert-service-api-docs")
                .route(RequestPredicates.path("/docs/alert-service/v3/api-docs"), http())
                .before(uri("http://alert-service:8084"))
                .filter(setPath("/v3/api-docs"))
                .build();
    }
}