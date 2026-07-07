package com.wallet.util;

import org.slf4j.MDC;

import java.util.UUID;

public class TraceUtil {

    private static final String TRACE_ID = "traceId";

    public static String generateTraceId() {
        return UUID.randomUUID().toString();
    }

    public static void setTraceId(String traceId) {
        MDC.put(TRACE_ID, traceId);
    }

    public static String getTraceId() {
        return MDC.get(TRACE_ID);
    }

    public static void clear() {
        MDC.clear();
    }
}