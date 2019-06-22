package com.yifeng.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author yifengguo
 *
 * simulate source of input messages for flink engine
 */
public class KafkaProducerProcess {
    public static void main(String[] args) throws Exception {
        // initialize kafka producer
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        String topic = "allData";
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // example message
        //{"dt":"2018-01-01 10:11:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.2,"level":"B"}]}

        // produce msgs
        while (true) {
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"countryCode\":\"" + getCountryCode() + "\",\"data\":[{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"},{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"}]}";
            System.out.println(message);
            producer.send(new ProducerRecord<>(topic, message));  //send msg to kafka
            TimeUnit.SECONDS.sleep(2);
        }
    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getCountryCode(){
        String[] types = {"US","TW","HK","PK","KW","SA","IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomType(){
        String[] types = {"s1","s2","s3","s4","s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static double getRandomScore(){
        double[] types = {0.3,0.2,0.1,0.5,0.8};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomLevel(){
        String[] types = {"A","A+","B","C","D"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
