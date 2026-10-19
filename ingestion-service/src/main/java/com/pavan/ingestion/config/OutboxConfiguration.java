package com.pavan.ingestion.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class OutboxConfiguration {

    @Bean
    public ObjectMapper outboxObjectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
