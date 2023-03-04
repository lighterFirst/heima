package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 工具类，处理缓存击穿
 */

@Component
@Slf4j
public class CacheUtil {

    // 创建线程池
    private static final ExecutorService CACHE_REBUIld__EXECTOR= Executors.newFixedThreadPool(10);

    private final StringRedisTemplate redisTemplate;

    public CacheUtil(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    // 逻辑过期，防止缓存击穿
    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit){
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis中
        redisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    // get 解决缓存穿透，



    /**
     * 缓存穿透
     * @param id
     * @return
     */
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = keyPrefix + id;
        // 1: 从redis查询商铺缓存
        String json = redisTemplate.opsForValue().get(key);
        // 2: 判断是否存在
        if(StringUtils.hasText(json)){
            // 3: 存在，直接返回
            return JSONUtil.toBean(json, type);

        }

        // 如果存入的是空值(解决缓存击穿)，返回错误
        if("".equals(json)){
            return null;
        }

        // 查询数据库，取出商铺
        R r = dbFallback.apply(id);

        // 不存在，返回错误
        if(Objects.isNull(r)){
            redisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        // 存在，写入缓存中

        this.set(key,JSONUtil.toJsonStr(r),time,unit);

        return r;

    }


    /**
     * 逻辑过期 取数据
     * @param id
     * @return
     */
    // 逻辑过期

    public <R,ID> R queryWithLogicalExpire(
            ID id, String keyPrefix, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        // 从缓存中获取商铺
        String key = keyPrefix+id;
        String json = redisTemplate.opsForValue().get(key);
        // 判断是否存在
        if (StrUtil.isBlank(json)){
            // 不存在，直接返回
            return null;
        }
        // 4:命中，将json转化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        // 获取店铺
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject, type);
        // 5：判断是否过期
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1：未过期，直接返回redis信息
            return r;
        }

        // 5.2 已过期，需要从数据库查询，重新缓存
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        // 6: 获取互斥锁
        boolean flag = tryLock(lockKey);
        // 6.1: 判断是否获取锁成功
        if(flag){
            // 注意：获取锁成功后应该再次监测redis缓存是否过期，做 doubleCheck，如果存在则无需重建缓存
            // 6.2: 成功，建立新的线程，实现缓存创建
            CACHE_REBUIld__EXECTOR.submit(()->{
                try{
                    // 查询数据库，取出商铺
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key,r1,time,unit);
                }catch (Exception e){
                    throw  new RuntimeException(e);
                }finally {
                    this.deleteLock(lockKey);
                }
            });
        }


        // 6.3: 失败，返回过期的数据
        return  r;


    }


    /**
     * 添加互斥锁
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = redisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 使用hutool工具包解决拆箱为空值的问题
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放互斥锁
     * @param key
     */
    private void deleteLock(String key){
        redisTemplate.delete(key);
    }
}
















