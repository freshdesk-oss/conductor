package com.netflix.conductor.client.aspect;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import java.util.HashMap;
import java.util.Map;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.client.automator.TextMapGetterHelper;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapGetter;

@Aspect
@Component
public class TracingAspect {

    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("conductor-client");
    private static final Logger LOGGER = LoggerFactory.getLogger(TracingAspect.class);

    // Pointcut to intercept methods with a specific annotation or all methods in a package
    @Pointcut("execution(* com.netflix.conductor.client.*(String traceParent, String spanName,..))")  // Target methods in 'conductor.client' package
    public void traceableMethods() {}

    // Before advice: starts the trace before the method execution
    @Before("traceableMethods()")
    public void startTracing(JoinPoint joinPoint) {
        LOGGER.info("inside startTracing"); 
        String traceParent = (String) joinPoint.getArgs()[0];
        String spanName = (String) joinPoint.getArgs()[1];
        Map<String, String> headers = new HashMap<>();
        headers.put("traceparent", traceParent);
        TextMapPropagator propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
        Context context = propagator.extract(Context.current(), headers, new TextMapGetterHelper());
        Span span = tracer.spanBuilder(spanName).setParent(context).startSpan();
        Scope scope = span.makeCurrent();
    }

    // After advice: ends the trace after the method execution
    @After("traceableMethods()")
    public void endTracing(JoinPoint joinPoint) {
        LOGGER.info("inside endTracing"); 
        Span.current().end();
    }
}
