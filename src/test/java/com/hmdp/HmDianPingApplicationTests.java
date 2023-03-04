package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.SystemConstants;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {



    @Autowired
    private ShopServiceImpl shopService;

    @Autowired
    private RedisWorker redisWorker;

    @Value("${spring.redis.host}")
    private String redisHost;


    @Test
    void testRedisHost(){
        System.out.println(redisHost);
    }

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for(int i = 0; i<100;i++){
                long id = redisWorker.nextId("order");
                System.out.println(id);
            }
            latch.countDown();
        };

        long begin = System.currentTimeMillis();
        for(int i =0;i<300;i++){
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end - begin);
    }

    @Test
    void test01() throws InterruptedException {
        shopService.saveShopToRedis(1L,10);
    }








}
