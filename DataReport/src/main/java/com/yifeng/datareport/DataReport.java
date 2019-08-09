package com.yifeng.datareport;

/**
 * Created by guoyifeng on 8/2/19
 */

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 需求：主要针对直播/短视频平台审核指标的统计
 * 1：统计不同大区每1 min内过审(上架)的数据量
 * 2：统计不同大区每1 min内未过审(下架)的数据量
 * 3：统计不同大区每1 min内加黑名单的数据量
 */
public class DataReport {
    public static void main(String[] args) throws Exception {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // kafka source
        DataStream<String> source = env.addSource(prepareKafkaSource());
    }

    private static FlinkKafkaConsumer011<String> prepareKafkaSource() {
        String topic = "auditLog";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop184:9200");
        props.setProperty("group.id", "con1");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        return consumer;
    }
}
