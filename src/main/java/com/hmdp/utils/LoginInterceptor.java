package com.hmdp.utils;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Objects;

public class LoginInterceptor implements HandlerInterceptor {


    /**
     * 进入controller，之前，做登录校验
     * @param request
     * @param response
     * @param handler
     * @return
     * @throws Exception
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
      // 1:判断是否需要拦截
        if(Objects.isNull(UserHolder.getUser())){
            response.setStatus(401);
            return false;
        }
        return true;
    }
}
