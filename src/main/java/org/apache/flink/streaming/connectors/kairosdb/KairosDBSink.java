package org.apache.flink.streaming.connectors.kairosdb;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;

public class KairosDBSink<T> extends RichSinkFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(KairosDBSink.class);
    /**
     * The client for communicating with KairosDB.
     */
    private HttpClient kairosdbClient;

    /**
     * The user specified config map that we forward to Elasticsearch when we create the Client.
     */
    private final Map<String, String> userConfig;

    /**
     * The builder that is used to construct an  from the incoming element.
     */
    private final KairosdbRequestBuilder<T> kairosdbRequestBuilder;

    public KairosDBSink(Map<String, String> userConfig, KairosdbRequestBuilder<T> kairosdbRequestBuilder){
        this.kairosdbRequestBuilder = kairosdbRequestBuilder;
        this.userConfig = userConfig;
    }

    public void open(Configuration configuration) throws InterruptedException {
        ParameterTool params = ParameterTool.fromMap(userConfig);
        try {
            kairosdbClient = new HttpClient("http://" + params.get("KariosDB.host") + ":" + params.getInt("KariosDB.port"));
        } catch (MalformedURLException e) {
            LOG.error("Kairos DB reported error, Malformed URL Error Exception" + e);
        }
    }

    @Override
    public void invoke(T element) throws Exception {
        KairosdbParser parser = kairosdbRequestBuilder.parse(element, getRuntimeContext());
        KairosdbConverter kairosconverter = new KairosdbConverter();
        try{
            kairosconverter.add(parser);
        }catch (Exception e){
            LOG.error("Error message: " + e);

        }
        this.sendMetric(kairosconverter.convert());
    }

    private void sendMetric(MetricBuilder metricBuilder) {
        try {
            Response response = kairosdbClient.pushMetrics(metricBuilder);

            //check if response is ok
            if(response.getErrors().size()>0){
                for(String e : response.getErrors()) {
                    LOG.error("KairosDB Response error:" + e);
                }
            }
        } catch (IOException e) {
            LOG.error("Could not request KairosDB.", e);
        } catch (URISyntaxException e) {
            LOG.error("Kairos DB reported error,URI Syntax Error Exception" + e);
        }
    }

    public void close(){
        if (kairosdbClient != null){
            try {
                kairosdbClient.shutdown();
            } catch (IOException e) {
                LOG.error("Kairos DB reported error, IO Exception" + e);
            }
        }
    }

}
