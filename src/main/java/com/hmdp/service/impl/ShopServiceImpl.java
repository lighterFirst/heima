package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.math.MathUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheUtil;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private CacheUtil cacheUtil;

    // 创建线程池
    private static final ExecutorService CACHE_REBUIld__EXECTOR= Executors.newFixedThreadPool(10);

    /**
     * 使用空解决redis缓存穿透问题，使用互斥锁解决缓存击穿
     * @param id
     * @return
     */
    @Override
    public Result queryShopById(Long id) {

        // 缓存穿透

        // 使用互斥锁解决缓存击穿,提取出的方法
        Shop shop = cacheUtil.queryWithLogicalExpire(id, RedisConstants.CACHE_SHOP_KEY, Shop.class, id2 -> {
            return getById(id2);
        }, RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);

        if (Objects.isNull(shop)){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);

        // 缓存击穿，使用提取出的方法解决
       /* Shop shop = cacheUtil.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, id2 -> {
            return getById(id2);
        }, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (Objects.isNull(shop)){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);*/

        // 缓存穿透

        // 使用互斥锁解决缓存击穿
       /* Shop shop = queryWithPassThrough(id);
        if(Objects.isNull(shop)){
            return Result.ok("店铺不存在！");
        }*/

       // 使用逻辑过期解决缓存击穿
       /* Shop shop = queryWithLogicalExpire(id);
        if (Objects.isNull(shop)){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);*/


    }

    // 逻辑过期

    public Shop queryWithLogicalExpire(Long id){
        // 从缓存中获取商铺
        String key = RedisConstants.CACHE_SHOP_KEY+id;
        String shopJson = redisTemplate.opsForValue().get(key);
        // 判断是否存在
        if (StrUtil.isBlank(shopJson)){
            // 不存在，直接返回
            return null;
        }
        // 4:命中，将json转化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        // 获取店铺
        JSONObject jsonObject = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(jsonObject, Shop.class);
        // 5：判断是否过期
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1：未过期，直接返回redis信息
            return shop;
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
                    this.saveShopToRedis(id,20);
                }catch (Exception e){
                    throw  new RuntimeException(e);
                }finally {
                    this.deleteLock(lockKey);
                }
            });
        }


        // 6.3: 失败，返回过期的数据
        return  shop;


    }

    /**
     * 逻辑时间，数据预热
     * @param id
     * @param expireSeconds
     */
    public void saveShopToRedis(Long id,long expireSeconds) throws InterruptedException {
        //测试： 休息200ms
        Thread.sleep(200);
        // 1:查询店铺数据
        Shop shop = getById(id);
        // 3:封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 4:写入redis
        redisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1: 从redis查询商铺缓存
        String shopJson = redisTemplate.opsForValue().get(key);
        // 2: 判断是否存在
        if(StringUtils.hasText(shopJson)){
            // 3: 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        // 如果存入的是空值(解决缓存击穿)，返回错误
        if("".equals(shopJson)){
            return null;
        }

        // 4: 不存在，根据id查询数据库

        // 使用互斥锁解决缓存穿透问题

        // 4.1: 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        boolean flag = tryLock(lockKey);

        Shop shop = null;
        // 4.2 线程没有获取到，休眠一段时间，再从缓存查

        try {
            if (!flag) {
                Thread.sleep(50);
                // 递归，直到从缓存中查取到数据或者拿到互斥锁
                return queryWithPassThrough(id);
            }

            // 4.3 线程成功获取到互斥锁，查询数据库并写入redis中。释放互斥锁

             shop = getById(id);

            // 模拟重建延时
            Thread.sleep(200);

            // 5: 数据库不存在(解决缓存击穿,保存在redis中，设置有效期2 min)，报错
            if (Objects.isNull(shop)) {
                redisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6: 存在，写入redis中，并把结果返回给前端
            redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            // 释放互斥锁
            deleteLock(lockKey);
        }
        return shop;

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

    /**
     * 写入数据库 (保持数据库与缓存的一致性),先更新数据库，再删除缓存
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result updateByShop(Shop shop) {
        if(Objects.isNull(shop.getId())){
            return Result.fail("该店铺不存在");
        }
        updateById(shop);
        String key = RedisConstants.CACHE_SHOP_KEY + shop.getId();
        redisTemplate.delete(key);
        return Result.ok();
    }
}
