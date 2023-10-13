package com.happening.kafka.utils;

public class ErrorUtils {

    private ErrorUtils() {
    }

    public static String getMessage(Throwable t) {
        var cause = t.getCause();
        if (cause != null && cause.getMessage() != null) {
            return cause.getMessage();
        }
        return t.getMessage();
    }

}
