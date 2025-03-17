package com.netflix.conductor.core.central.client;

import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class UnirestHttpClient implements HttpClient {

    @Override
    public HttpResponse<String> post(String url, Map<String, String> headers, String body) {
        return Unirest.post(url)
                .headers(headers)
                .body(body)
                .asString();
    }
}
