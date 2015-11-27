package org.apache.flink.streaming.connectors.kairosdb;

import java.util.Map;

public class KairosdbParser {

    public final String name;
    public final Map<String, String> tags;
    public final long time;
    public final Object value;

    public KairosdbParser(String name, Map<String, String> tags, long time, Object value) {
        this.name = name;
        this.tags = tags;
        this.time = time;
        this.value = value;
    }

}