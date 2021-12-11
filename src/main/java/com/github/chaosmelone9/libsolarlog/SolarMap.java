package com.github.chaosmelone9.libsolarlog;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Main Class for runtime data storage
 * @author Christoph Kohnen
 * @since 0.0.0rc0.2-0
 */
public class SolarMap {
    private final Map<Inverter, Map<Date, Map<String, Integer>>> data;

    public SolarMap() {
        this.data = new HashMap<>();
    }

    public void addFromMap(Map<Inverter, Map<Date, Map<String, Integer>>> map) {
        map.forEach(this::addData);
    }

    private void addData(Inverter inverter, Map<Date, Map<String, Integer>> map) {
        map.forEach(data.get(inverter)::putIfAbsent);
    }

    public Map<Inverter, Map<Date, Map<String, Integer>>> getData() {
        return data;
    }
}
