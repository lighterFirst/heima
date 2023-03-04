package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.lang.UUID;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.ShopTypeDto;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public Result listForShop() {
        // 1；查询redis缓存，有无数据
        String key = RedisConstants.CACHE_SHOPTYPE_KEY;
        String listShopByRedis = redisTemplate.opsForValue().get(key);

        if(StringUtils.hasText(listShopByRedis)){
            List<ShopType> shopTypes = JSONUtil.toList(listShopByRedis, ShopType.class);
            return Result.ok(shopTypes);
        }
        // 2: 没有，查询数据库，
        LambdaQueryWrapper<ShopType> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.orderByAsc(ShopType::getSort);
        List<ShopType> listShopBySql = list(lambdaQueryWrapper);
        // 3: 将list集合放入redis缓存中
        redisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(listShopBySql));
        // 4: 返回结果
        return Result.ok(listShopBySql);
    }


}
