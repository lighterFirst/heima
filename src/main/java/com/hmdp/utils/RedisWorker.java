package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 生成订单唯一id
 */

@Component
public class RedisWorker {

    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * 开始时间戳
     * @param keyPrefix
     * @return
     */
    private static final long BEGIN_TIMESTAMP = 1672531200L;

    /**
     * 记录序列号位数
     * @param keyPrefix
     * @return
     */

    private static final int COUNT_BITS = 32;

    public long nextId(String keyPrefix){
        // 1: 生成时间戳
        LocalDateTime nowTime = LocalDateTime.now();
        long nowSeconds = nowTime.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSeconds - BEGIN_TIMESTAMP;

        // 2:生成序列号
        String formatTime = nowTime.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = redisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + formatTime);
        // 3:拼接并返回
        return timestamp << COUNT_BITS | count;

    }

    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2023, 1, 1, 0, 0, 0);
        long seconds = time.toEpochSecond(ZoneOffset.UTC);
        LocalDateTime nowTime = LocalDateTime.now();
        System.out.println( nowTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

}
