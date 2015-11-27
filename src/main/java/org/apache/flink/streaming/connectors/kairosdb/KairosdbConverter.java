package org.apache.flink.streaming.connectors.kairosdb;

import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;

public class KairosdbConverter {
    private final MetricBuilder metricBuilder;

    public KairosdbConverter() {
        this.metricBuilder = MetricBuilder.getInstance();
    }

    public KairosdbConverter add(KairosdbParser parser) {
        Metric kairosMetric = metricBuilder.addMetric(parser.name)
                .addDataPoint(parser.time, parser.value)
                .addTags(parser.tags);
        return this;
    }

    public MetricBuilder convert() {
        return metricBuilder;
    }


}