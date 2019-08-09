package com.yifeng.dataclean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yifeng.dataclean.source.MyRedisSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author yifengguo
 *
 * glue code for streaming process
 *
 * bin/kafka-topics.sh  --create --topic allData --zookeeper localhost:2181 --partitions 5 --replication-factor 1
 * bin/kafka-topics.sh  --create --topic allDataClean --zookeeper localhost:2181 --partitions 5 --replication-factor 1
 *
 * run by script
 * flink run -m localhost:6123 -d /data/soft/jars/DataClean/DataClean-1.0-SNAPSHOT-jar-with-dependencies.jar
 */
public class DataClean {

    public static void main(String[] args) throws Exception {
        // get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(5);  // sync with kafka topic partitions num

        //  config of checkpoint
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //  statebackend config
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));

        // configs for kafka source
        String topic = "allData";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "con1");
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);

        // source from kafka
        // {"dt":"2018-01-01 11:11:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.1,"level":"B"}]}
        DataStreamSource<String> data = env.addSource(kafkaConsumer);

        // source from redis: latest mapping between CountryCode and AreaCode
        // this source is to record mapping between CountyCode and AreaCode which can be changed over time
        // .broadcase() is to make sure each thread of CoFlatMapFunction() could get mapping info from redis source thread
        // otherwise some thread may not get mapping info and then output of CoFlatMapFunction() may have missing field
        DataStream<Map<String, String>> mapData = env.addSource(new MyRedisSource()).broadcast();

        SingleOutputStreamOperator<String> flatData = data.connect(mapData)
                .flatMap(new CoFlatMapFunction<String, Map<String, String>, String>() {

                    Map<String, String> mappingMap = new HashMap<>();

                    // process log data from kafka
                    public void flatMap1(String value, Collector<String> out) throws Exception {
                        JSONObject obj = JSONObject.parseObject(value);
                        String countryCode = obj.getString("countryCode");
                        String dt = obj.getString("dt");
                        String areaCode = mappingMap.get(countryCode);

                        JSONArray jsonArray = obj.getJSONArray("data");
                        // update item in data
                        for (int i = 0; i < jsonArray.size(); i++) {
                            JSONObject curr = jsonArray.getJSONObject(i);
                            curr.put("area", areaCode);
                            curr.put("dt", dt);
                            out.collect(JSONObject.toJSONString(curr));  // collect updated item in data[]
                        }
                    }

                    // process mappings from redis
                    public void flatMap2(Map<String, String> value, Collector<String> out) throws Exception {
                        this.mappingMap = value;
                    }
                });

        // exactly-once semantic
        String sinkTopic = "allDataClean";
        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "localhost:9092");
        sinkProps.setProperty("transaction.timeout.ms",60000*15+"");


        FlinkKafkaProducer011<String> kafkaSink = new FlinkKafkaProducer011<>(
                sinkTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                sinkProps,
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        flatData.addSink(kafkaSink);

        // monitor topic sink info
        // ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic allDataClean

        // execute
        env.execute("DataClean");
    }
}
