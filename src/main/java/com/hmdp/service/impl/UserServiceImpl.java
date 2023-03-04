package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * 生成手机验证码，并发送给用户
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1: 校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            // 2: 如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }

        // 3: 符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 4：保存验证码  (之前放到session里)
        redisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY +phone,code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 发送验证码
        log.debug("发送验证码成功，验证码：{}",code);
        // 5：返回ok
        return Result.ok();
    }

    /**
     * 将用户信息保存到session里
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1: 校验手机号 (之前从session中获取，现在从redis中获取)
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式不对");
        }
        // 2: 校验验证码, 将session的验证码和前端传过来的进行比较
        String ridesCode = redisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY +phone);
        String code = loginForm.getCode();
        if(ridesCode == null || !ridesCode.equals(code)){
            // 3: 不一致
            return Result.fail("验证码不正确");
        }

        // 4: 一致，根据手机号查询用户
        LambdaQueryWrapper<User> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(User::getPhone, phone);
        User user = getOne(lambdaQueryWrapper);

        // 5: 判断用户名是否存在
        if(Objects.isNull(user)){
            // 6:不存在，创建用户
            user = createUserByPhone(phone);

        }

        // 7: 将用户信息保存到redis中 (之前保存用户信息到 session)，key为uuid，value为user对象

        // 生成uuid, 不带下划线
        String token = UUID.randomUUID().toString(true);
        // 将user对象转化为hash
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 将userDto转化为map集合
        Map<String, Object> userDtoMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                            .setIgnoreNullValue(true)
                            .setFieldValueEditor((fieldName,fieldValue)->
                                fieldValue.toString()
                            ));
        redisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY+token,userDtoMap);
        // 设置token有效期
        redisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.LOGIN_USER_TTL,TimeUnit.MINUTES);
        // 需要返回token (user对象的key)，(之前不用返回登录凭证（jwt），返回sessionId)
        return Result.ok(token);
    }

    /**
     * 保存用户
     * @param phone
     * @return
     */
    private User createUserByPhone(String phone) {

        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX +RandomUtil.randomString(5));
        save(user);
        return user;

    }
}
