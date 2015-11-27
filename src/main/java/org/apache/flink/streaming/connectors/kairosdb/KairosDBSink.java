package org.apache.flink.streaming.connectors.kairosdb;

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
     * The server of the kairos db.
     */
    protected final String server;
    /**
     * The port of the kairos db.
     */
    protected final int port;


    /**
     * The builder that is used to construct an  from the incoming element.
     */
    private final KairosdbRequestBuilder<T> kairosdbRequestBuilder;

    public KairosDBSink(Map<String, String> userConfig, KairosdbRequestBuilder<T> kairosdbRequestBuilder){
        this.kairosdbRequestBuilder = kairosdbRequestBuilder;
        this.server = userConfig.get("kairosdb.server");
        this.port = Integer.parseInt(userConfig.get("port"));

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
            HttpClient client = new HttpClient("http://" + this.server + ":" + this.port);
            Response response = client.pushMetrics(metricBuilder);

            //check if response is ok
            if (response.getStatusCode() != 200) {
                LOG.error("Kairos DB reported error. Status code: " + response.getStatusCode());
                LOG.error("Error message: " + response.getErrors());
            } else {
                LOG.debug("Kairos DB returned OK. Status code: " + response.getStatusCode());
            }
            client.shutdown();
        } catch (MalformedURLException e) {
            LOG.error("Kairos DB reported error, Malformed URL Error Exception" + e);
        } catch (IOException e) {
            LOG.error("Could not request KairosDB.", e);
        } catch (URISyntaxException e) {
            LOG.error("Kairos DB reported error,URI Syntax Error Exception" + e);
        }
    }

}
