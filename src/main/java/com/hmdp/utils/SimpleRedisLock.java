package com.hmdp.utils;


import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    // 自定义key
    private String name;
    private StringRedisTemplate redisTemplate;
    // key名前缀
    private static final String KEY_PREFIX = "lock:";
    // 提前读好redis脚本，避免io读写
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    // 脚本在类加载时执行
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("UNLOCK.lua"));
        UNLOCK_SCRIPT.setResultType(long.class);
    }

    // 线程id随着jvm创建而递增，如果我们有多个jvm。线程池id可能会重复。这里我们使用uuid
    // 类加载时执行，每个线程都有一个相同的 ID_PREFIX
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    public SimpleRedisLock(String name, StringRedisTemplate redisTemplate) {
        this.name = name;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        Boolean flag = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(flag);
    }

    @Override
    public void unLock() {
        // 调用lua脚本
        redisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX+name),
                ID_PREFIX + Thread.currentThread().getId()
        );
    }

    /* @Override
    public void unLock() {
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 释放锁前，需要判断线程是否是之前得到锁的线程 （在redis的value中保留了uuid和线程id）
        String id = redisTemplate.opsForValue().get(KEY_PREFIX + name);
        if(threadId.equals(id)){
            redisTemplate.delete(KEY_PREFIX+name);
        }

    }*/

}











