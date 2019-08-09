package com.yifeng.dataclean.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author yifengguo
 *
 * initialize data in the redis
 *
 * hset areas AREA_US US
 * hset areas AREA_CT TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 *
 * return the mapping between CountryCode and AreaCode by a hashmap
 * remember start redis service
 */
public class MyRedisSource implements SourceFunction<Map<String, String>> {

    private boolean isRunning = true;

    private Jedis jedis;

    private static final Logger LOG = LoggerFactory.getLogger(MyRedisSource.class);

    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        jedis = new Jedis("localhost", 6379);

        Map<String, String> outMap = new HashMap<>();

        while (isRunning) {
            try {
                // fetch mapping from redis
                Map<String, String> areas = jedis.hgetAll("areas");  // get latest data at each time
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String areaCode = entry.getKey();
                    String[] countryCodes = entry.getValue().split(",");
                    for (String countryCode : countryCodes) {
                        outMap.put(countryCode, areaCode);
                    }
                }

                if (outMap.size() > 0) {
                    sourceContext.collect(outMap);
                }

                TimeUnit.MINUTES.sleep(1);  // fetch latest mappings from redis once a minute
            } catch (JedisConnectionException e) {  // if connection to redis throws an exception, try one more time
                LOG.warn("exception in connecting to redis due to {}, reconnecting...", e.getCause());
                jedis = new Jedis("localhost", 6379);
            } catch (Exception e) {
                LOG.error("error in getting mappings from redis {}", e);
            }
        }
    }

    public void cancel() {
        isRunning = false;

        if (jedis != null) {
            jedis.close();
        }
    }
}
