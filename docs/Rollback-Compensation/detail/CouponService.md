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
        // 第0步：业务幂等，先查这用户是不是已经领过
        Long already=userCouponMapper.selectCount(
                // WHERE user_id=? AND coupon_code=?
                new LambdaQueryWrapper<UserCoupon>()
                        .eq(UserCoupon::getUserId, userId)
                        .eq(UserCoupon::getCouponCode, couponCode));
        // 已经领过
        if (already != null && already > 0) {
            // 409 直接拒绝
            throw BizException.conflict("您已领取过该优惠券");
        }
        // 第0.5步：并发去重，挡"同一用户同一券瞬间点两次"
        // 幂等 key：谁+哪张券
        String idemKey="coupon:acquire"+userId+":"+couponCode;
        // SETNX：10s窗口内第一个请求成功，后来者返回false
        Boolean firstRequest=redisTemplate.opsForValue()
                .setIfAbsent(idemKey, "1", 10, TimeUnit.SECONDS);
        // key 已被占
        if (Boolean.FALSE.equals(firstRequest)) {
            // 快速失败，不进DB
            throw BizException.conflict("操作过于频繁，请稍后再试");
        }
        // 第一步：从DB读券，DB是库存权威来源
        // 按券码查一条
        Coupon coupon=couponMapper.selectOne(
                new LambdaQueryWrapper<Coupon>().eq(Coupon::getCouponCode, couponCode));
        // 券不存在，或 DB 库存为0
        if (coupon==null || coupon.getRemainingCount() <= 0) {
            throw BizException.notFound("优惠券不存在或已过期");
        } 
        // 有结束时间 且 已过
        if (coupon.getEndTime()!=null && coupon.getEndTime().isBefore(LocalDateTime.now())) {
            throw BizException.badParam("优惠券已过期");
        }
        // 第2步：Redis 库存回源（必须在decrement之前）
        // Redis 没这个键时，用 DB 当前库存建出来
        seedStockFromDbIfAbsent(couponCode, coupon.getRemainingCount());

        // 第3步：Redis 原子预扣，高并发第一道闸门
        // 拼完整库存 key
        String stockKey=STOCK_KEY_PREFIX+couponCode;
        // DECR 原子减一，返回减后的值
        Long remain=redisTemplate.opsForValue().decrement(stockKey);
        // 减成负数
    }
}

```
