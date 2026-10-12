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
    
    // 操作 coupon 表（库存权威源）
    private final CouponMapper couponMapper;
    // 操作 user_coupon 表（一人一单记录）
    private final UserCouponMapper userCouponMapper;
    // Redis，扛高并发预扣
    private final StringRedisTemplate redisTemplate;
    // 库存 key 前缀，完整 key = 前缀 + 券码
    private static final String STOCK_KEY_PREFIX = "coupon:stock:";

    //
    @Transactional(rollbackFor=Exception.class)
    // 领券主入口：成功 true，失败抛 BizException
    public boolean acquireCoupon(String userId, String couponCode) {
        // 第0步：业务幂等，先查这用户是不是已经领过
        Long already=userCouponMapper.selectCount(
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
        // 有结束时间且已过期
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
        // 减成负数=超卖了
        if (remain==null || remain<0) {
            // 反悔：把减的加回去
            redisTemplate.opsForValue().increment(stockKey);
            throw BizException.conflict("优惠券已抢光");
        }

        // 第4步：注册回滚钩子，后面 DB 失败回滚时补回 Redis
        // 把回调绑定到当前事务
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            // 事务结束（提交/回滚）都会调
            @Override
            public void afterCompletion(int status) {
                // 只有被回滚才补偿（提交是 STATUS_COMMITTED）
                if (status == STATUS_ROLLED_BACK) {
                    // 抵消第 3 步那次 decrement
                    redisTemplate.opsForValue().increment(stockKey);
                    // 释放幂等键，让用户能重试
                    redisTemplate.delete(idemKey);
                    log.warn("[Coupon] rollback — Redis stock restored | code={}", couponCode);
                }
            }
        });
        // 第5步：DB 乐观锁最终裁决，复用第一步读到的 version
        int affected=couponMapper.decrementRemainingCount(couponCode, coupon.getVersion());
        if (affected==0) {
            throw BizException.conflict("优惠券库存不足，请稍后再试");
        }
        // 第6步：写领取记录
        // INSERT 一条
        userCouponMapper.issueToUser(userId, couponCode);
        log.info("[Coupon] acquired | userId={} | code={}", userId, couponCode);
        return true;
    }
}

```
