package com.netflix.conductor.client.automator;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

public class TextMapGetterHelper
        implements TextMapGetter<Map<String, String>> {
    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
        return carrier.keySet();
    }

    @Override
    public String get(Map<String, String> carrier, String key) {
        return carrier.get(key);
    }
}
