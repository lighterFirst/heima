package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    // 提前读好redis脚本，避免io读写
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 脚本在类加载时执行
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("SECKILL.lua"));
        SECKILL_SCRIPT.setResultType(long.class);  // 返回值
    }

    // 创建阻塞队列
   /* private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);*/

    // 创建单线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //@PostConstruct的作用是：类初始化时，会执行该方法。将VoucherOrderHandler线程放入线程池中
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    // 因为不是同一个线程，事务的代理是通过线程的拿到的
    private IVoucherOrderService proxy = null;

    // 创建线程
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while(true){
                try {
                    // 获取消息队列中的订单信息 ， xreadgroup group g1 c1 block 2000 stream.order > 创建g1组的c1消费者，阻塞2秒,队列是stream.order，> 最新消息
                    String queueName = "stream.orders";
                    String groupName = "g1";
                    List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream().read(
                            Consumer.from(groupName, "c1"),  // 组名和消费者名
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),  // 读一个，阻塞2s
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()) // 读取队列，最新消息
                    );
                    // 消息获取失败，结束，下一次循环
                    if(list == null || list.isEmpty()){
                        // 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }

                    // 解析消息，获取订单信息，list下标为消息的id（redis生成的），值为3个id
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    RecordId orderId = record.getId();
                    // 获取消息成功，下单
                    handVoucherOrder(voucherOrder);
                    // ack确认消息  sack stream.orders g1 id  确认 队列 消费者的组 消息的id （redis生成的）
                    redisTemplate.opsForStream().acknowledge(queueName,groupName,orderId);
                } catch (Exception e) {
                    log.error("处理异常失败",e);
                    handPendingList();
                }
            }
        }

        // 处理异常的消息，重新入队
        private void handPendingList() {

            while(true){
                try {
                    // 获取消息队列中的订单信息 ， xreadgroup group g1 c1  stream.order > 创建g1组的c1消费者，不需要阻塞了,队列是stream.order，0 读的是未确认消息（pendList）
                    String queueName = "stream.orders";
                    String groupName = "g1";
                    List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream().read(
                            Consumer.from(groupName, "c1"),  // 组名和消费者名
                            StreamReadOptions.empty().count(1),  // 读一个，阻塞2s
                            StreamOffset.create(queueName, ReadOffset.from("0")) // 读取队列，未确认消息
                    );
                    // 消息获取失败，结束，下一次循环
                    if(list == null || list.isEmpty()){
                        // 如果获取失败，说明pendingList没有消息，调出循环
                        break;
                    }

                    // 解析消息，获取订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    RecordId orderId = record.getId();
                    // 获取消息成功，下单
                    handVoucherOrder(voucherOrder);
                    // ack确认消息  sack stream.orders g1 id  确认 队列 消费者的组 订单号
                    redisTemplate.opsForStream().acknowledge(queueName,groupName,orderId);
                } catch (Exception e) {
                    log.error("处理pending-list异常失败",e);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }


        }
        }

        // 创建线程
   /* private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while(true){
                try {
                    // 阻塞队列的take方法：获取和删除队列的头部，知道等待队列的元素可用
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 创建订单
                   handVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理异常失败",e);
                }
            }
        }
    }*/

    private void handVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("userid:" + userId, redisTemplate);
        RLock lock = redissonClient.getLock("Lock:order:" + userId);
        boolean isLock = lock.tryLock();
        // 获取锁
        if(!isLock){
            // 获取锁失败,说明该用户已经有线程抢到锁了
            log.error("您已经抢到锁了");

        }

        // 释放锁
        try{
            // 获取代理对象
          /*  IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();*/
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            // 释放锁
            simpleRedisLock.unLock();
        }

    }

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private RedisWorker redisWorker;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private RedissonClient redissonClient;


    /**
     * 升级，将jdk的阻塞队列，升级为redis的stream 队列,主线程方法
     * @param voucherId
     * @return
     */
    @Override
    public Result seckill(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisWorker.nextId("order");
        // 1:执行lua脚本,将订单信息的id，优惠券id，用户id放入消息队列中去
        Long result = redisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        // 2:判断结果是否为0
        if(result.intValue() != 0){
            // 3: 不为0，没有购买资格
            return Result.fail(result == 1 ?"库存不足":"您已经下过单了");
        }
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    /**
     * 主线程，在redis中判断优惠券是否可用，将订单添加到阻塞队列
     * @param
     * @return
     */
   /* @Override
    public Result seckill(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1:执行lua脚本
        Long result = redisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        // 2:判断结果是否为0
        if(result.intValue() != 0){
            // 3: 不为0，没有购买资格
            return Result.fail(result == 1 ?"库存不足":"您已经下过单了");
        }
        // 4: 下单，将下单信息保存到异步队列
        long orderId = redisWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        // 4.1: 返回订单id
        voucherOrder.setId(orderId);
        // 4.2：用户id
        voucherOrder.setUserId(userId);
        // 4.3: 代金券id
        voucherOrder.setVoucherId(voucherId);
        // 4.4 放入阻塞队列
        orderTasks.add(voucherOrder);
        // 5:获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }*/

    /* @Override
    public Result seckill(Long voucherId) {

        // 查询id
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        // 查看秒杀是否开始
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        if(beginTime.isAfter(LocalDateTime.now())){
            return Result.fail("秒杀活动未开始");
        }
        // 判断秒杀是否结束
        LocalDateTime endTime = seckillVoucher.getEndTime();
        if(endTime.isBefore(LocalDateTime.now())){
            return Result.fail("秒杀活动已结束");
        }

        // 判断库存是否充足
        if(seckillVoucher.getStock() <1){
            return Result.fail("库存不足");
        }

        // 实现一人一单
        // 锁用户id,升级版，分布式锁
        Long userId = UserHolder.getUser().getId();
      *//*  synchronized (userId.toString().intern()) {*//*

        //创建锁对象
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("userid:" + userId, redisTemplate);
        RLock lock = redissonClient.getLock("Lock:order:" + userId);
        boolean isLock = lock.tryLock();
        // 获取锁
        *//*boolean flag = simpleRedisLock.tryLock(3000);*//*
        if(!isLock){
            // 获取锁失败,说明该用户已经有线程抢到锁了
            return Result.fail("您已经抢到锁了");

        }

        // 释放锁
        try{

            // 获取代理对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }finally {
            // 释放锁
            simpleRedisLock.unLock();
        }

        *//*}*//*

    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        // 锁用户id
        Long userId = voucherOrder.getUserId();

            // 查询订单列表，检查该用户是否拥有优惠券
            LambdaQueryWrapper<VoucherOrder> lambdaQueryWrapper = new LambdaQueryWrapper<>();
            lambdaQueryWrapper.eq(VoucherOrder::getUserId,userId);
            lambdaQueryWrapper.eq(VoucherOrder::getVoucherId, voucherOrder.getVoucherId());
            int count = count(lambdaQueryWrapper);
            if(count > 0){
                log.error("您已经拥有优惠券了");
            }

            // 扣减库存，此处 判断库存（stock） > 0 ,可以保证不发生超卖
            boolean flag = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0).update();

            // 创建订单
            if(!flag){
                // 扣减失败
               log.error("库存不足");
            }
            save(voucherOrder);
        }



}













