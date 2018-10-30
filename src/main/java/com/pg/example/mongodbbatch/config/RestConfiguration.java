package com.pg.example.mongodbbatch.config;

import com.pg.example.mongodbbatch.util.Timeout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Created by Pawel Gawedzki on 21-Mar-18.*/
@Configuration
public class RestConfiguration {

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder, @Value("${request.connection.timeout}") String timeout) {
        return restTemplateBuilder
                .setConnectTimeout(Timeout.timeout(timeout))
                .setReadTimeout(Timeout.timeout(timeout))
                .additionalMessageConverters(new ByteArrayHttpMessageConverter())
                .build();
    }

    @Bean
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(10);
    }
}
