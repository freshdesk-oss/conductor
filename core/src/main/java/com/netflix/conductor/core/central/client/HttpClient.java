package com.netflix.conductor.core.central.client;

import kong.unirest.HttpResponse;

import java.util.Map;

public interface HttpClient {
    HttpResponse<String> post(String url, Map<String, String> headers, String body);
}
