package com.happening.kafka.utils;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public final class PropertiesUtils {
    private PropertiesUtils() {
    }

    public static Properties toProperties(Map<Object, Object> map) {
        // need to remove nulls, because Properties doesn't support them
        map.values().removeAll(Collections.singleton(null));
        var result = new Properties();
        result.putAll(map);
        return result;
    }
}