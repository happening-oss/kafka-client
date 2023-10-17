package com.happening.kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public final class Utils {
    private Utils() {
    }

    public static String getErrorMessage(Throwable t) {
        var cause = t.getCause();
        if (cause != null && cause.getMessage() != null) {
            return cause.getMessage();
        }
        return t.getMessage();
    }

    public static Properties toProperties(Map<Object, Object> map) {
        // need to remove nulls, because Properties doesn't support them
        map.values().removeAll(Collections.singleton(null));
        var result = new Properties();
        result.putAll(map);
        return result;
    }

}
