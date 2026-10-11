# CouponService

```java
package com.shopmate.coupon.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.shopmate.common.exception.BizException;
import com.shopmate.coupon.model.entity.Coupon;
import com.shopmate.coupon.model.entity.UserCoupon;
import com.shopmate.coupon.repository.CouponMapper;
import com.shopmate.coupon.repository.UserCouponMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class CouponService {
    @Transactional(rollbackFor=Exception.class)
    // 领券主入口：成功true，失败抛 BizException
    public boolean acquireCoupon(String userId, String couponCode) {
        //第0步：业务幂等，先查这用户是不是已经领过
        Long already=userCouponMapper.selectCount(
        )
        // 已经领过
        if (already != null && already > 0) {
            // 409 直接拒绝
            throw BizException.conflict("您已领取过该优惠券");
        }
    }
}

```
