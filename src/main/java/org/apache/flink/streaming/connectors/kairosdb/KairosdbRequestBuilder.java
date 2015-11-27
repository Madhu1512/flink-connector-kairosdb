package org.apache.flink.streaming.connectors.kairosdb;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public interface KairosdbRequestBuilder<T> extends Function, Serializable {

    KairosdbParser parse(T element, RuntimeContext runtimeContext);

}