package com.pg.example.mongodbbatch.web;

import com.pg.example.mongodbbatch.util.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**Created by Pawel Gawedzki on 9/21/2017.*/
@Component
public class PalantirClient {

    private static final Logger logger = LoggerFactory.getLogger(PalantirClient.class);

    private final RestTemplate restTemplate;
    private final String token;
    private final ExecutorService executorService;
    private final boolean logWhileWaitingForResponse;
    private final String logWaitIntervalInSeconds;

    @Autowired
    public PalantirClient(RestTemplate restTemplate,
                          @Value("${palantir.token}") String token,
                          ExecutorService executorService,
                          @Value("${log.while.waiting.for.response}") String logWhileWaitingForResponse,
                          @Value("${log.wait.interval}") String logWaitIntervalInSeconds) {
        this.restTemplate = restTemplate;
        this.token = token;
        this.executorService = executorService;
        this.logWhileWaitingForResponse = "true".equals(logWhileWaitingForResponse);
        this.logWaitIntervalInSeconds = logWaitIntervalInSeconds;
    }

    public String getSchemaData(QueryColumnBuilder queryBuilder, String url) {
        validateToken();
        HttpEntity<String> entity = new HttpEntity<>(queryBuilder.build(), createHeadersWithMediaType());

        logger.info("REST Request sent to [{}] with details [{}].", url, queryBuilder.toString());
        if (logWhileWaitingForResponse) {
            ResponseEntity<String> responseEntity = logWhileWaiting(url, () -> restTemplate.postForEntity(url, entity, String.class));
            return responseEntity.getBody();
        } else {
            return restTemplate.postForEntity(url, entity, String.class).getBody();
        }
    }

    private void validateToken() {
        if (StringUtils.nullOrEmpty(token)) {
            throw new RuntimeException("Palantir token is not specified. Please update application.properties with proper value for the key 'gcss.token'.");
        }
    }

    private <T> T logWhileWaiting(String url, Callable<T> callable) {
        try {
            Future<T> future = executorService.submit(callable);
            long interval = getIntervalInSeconds();
            while (!future.isDone()) {
                logger.debug("Waiting to finish the [{}]", url);
                Thread.sleep(interval);
            }
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Something went wrong.", e);
            throw new RuntimeException(e);
        }
    }

    private long getIntervalInSeconds() {
        long interval = 1000;
        try {
            interval *= Long.parseLong(logWaitIntervalInSeconds);
        } catch (NumberFormatException e) {
            logger.warn("Wrong value for log.wait.interval [{}]. Default value for interval is 1 second.", logWaitIntervalInSeconds);
        }
        return interval;
    }

    private HttpHeaders createHeadersWithMediaType() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    public void downloadDataToFile(UrlBuilder urlBuilder, final String filePath) throws IOException {
        validateToken();

        String url = urlBuilder.build();

        logger.info("REST Request sent to [{}].", url);

        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            HttpGet request = new HttpGet(url);
            request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);

            HttpResponse response = client.execute(request);
            org.apache.http.HttpEntity entity = response.getEntity();

            int responseCode = response.getStatusLine().getStatusCode();

            logger.debug("Response Code: {}", responseCode);
            logger.debug("Saving response to file: [{}]", filePath);
            if (logWhileWaitingForResponse) {
                logWhileWaiting(url, (Callable<Void>) () -> {
                    FileUtils.copyInputStreamToFile(entity.getContent(), new File(filePath));
                    return null;
                });
            } else {
                FileUtils.copyInputStreamToFile(entity.getContent(), new File(filePath));
            }
            logger.debug("File download completed.");
        } catch (IOException e) {
            logger.error("", e);
            throw e;
        }

    }
}
