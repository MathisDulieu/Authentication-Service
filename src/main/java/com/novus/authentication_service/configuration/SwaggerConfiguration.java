package com.novus.authentication_service.configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.tags.Tag;
import org.springdoc.core.customizers.OperationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfiguration {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("SupMap REST API")
                        .description("SupMap API")
                        .license(new License().name("Apache License Version 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0.html"))
                        .version("1.0.0"))
                .addServersItem(new io.swagger.v3.oas.models.servers.Server()
                        .url("https://production-api-gateway.up.railway.app")
                        .description("Production Server"))
                .addServersItem(new io.swagger.v3.oas.models.servers.Server()
                        .url("http://localhost:8080")
                        .description("Local Server"))
                .components(new io.swagger.v3.oas.models.Components()
                        .addSecuritySchemes("bearerAuth",
                                new SecurityScheme()
                                        .type(SecurityScheme.Type.HTTP)
                                        .scheme("bearer")
                                        .bearerFormat("JWT")))
                .addTagsItem(new Tag().name("Authentication").description("Endpoints for user authentication and authorization."))
                .addTagsItem(new Tag().name("User").description("Endpoints for managing user accounts"))
                .addTagsItem(new Tag().name("Contact").description("Endpoints for user support and contact operations"))
                .addTagsItem(new Tag().name("Map").description("Endpoints for navigation, route calculation, and traffic incidents reporting"))
                .addTagsItem(new Tag().name("Notification").description("Endpoints for managing alerts, user notifications and real-time updates"));
    }

    @Bean
    public OperationCustomizer globalResponses() {
        return (operation, handlerMethod) -> {
            ApiResponses apiResponses = operation.getResponses() != null ? operation.getResponses() : new ApiResponses();
            apiResponses.addApiResponse("429", new ApiResponse()
                    .description("Too Many Requests")
                    .content(new Content()
                            .addMediaType("text/plain", new MediaType().example("Too many requests. Please try again later."))));
            operation.setResponses(apiResponses);
            return operation;
        };
    }

}
