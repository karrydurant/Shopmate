# OrderService

```java
package com.shopmate.order.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.shopmate.common.exception.BizException;
import com.shopmate.coupon.service.CouponService;
import com.shopmate.order.model.entity.Order;
import com.shopmate.order.model.entity.OrderItem;
import com.shopmate.order.model.entity.OrderStatusLog;
import com.shopmate.order.model.enums.OrderStatus;
import com.shopmate.order.repository.OrderItemMapper;
import com.shopmate.order.repository.OrderMapper;
import com.shopmate.order.repository.OrderStatusLogMapper;
import com.shopmate.product.model.entity.ProductSku;
import com.shopmate.product.repository.ProductSkuMapper;
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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderMapper orderMapper;
    private final OrderItemMapper orderItemMapper;
    private final ProductSkuMapper skuMapper;
    private final StringRedisTemplate redisTemplate;
    private final CouponService couponService;
    private final OrderStatusLogMapper statusLogMapper;

    // 状态流转审计里的操作者标识
    private static final String OPERATOR_USER = "user";
    private static final String OPERATOR_ADMIN = "admin";
    public static final String OPERATOR_SYSTEM = "system";

    private static final String IDEMPOTENCY_PENDING = "PENDING";
    private static final long IDEMPOTENCY_TTL_HOURS = 24;

    // 创建订单（无幂等键，仅按 orderNo 查重）
    public Order createOrder(Order order, List<OrderItem> items) {
        return createOrder(order, items, null);
    }

    /**
     * 创建订单——幂等+库存锁定
     *
     * 幂等保证：
     * requestId 幂等键：相同 requestId 只创建一次，重复请求返回已有订单
     * orderNo 查重：orders 表 uk_order_no 唯一索引兜底，防止重复落单
     */
    @Transactional(rollbackFor=Exception.class)
    public Order createOrder(Order order, List<OrderItem> items, String requestId) {
        if (order.getUserId() == null || order.getUserId().isBlank()) {
            throw BizException.badParam("用户不能为空");
        }
        // 幂等：requestId 的最终裁决是 orders(user_id, request_id) 唯一索引
        String idemKey=null;
        if (requestId!=null && !requestId.isBlank()) {
            idemKey = idempotencyKey(order.getUserId(), requestId);
            
        }
    }

    /**
     * 退款——状态机流转+退券，不碰资金、库存
     *
     * 为什么这里只有状态流转：
     * 1.项目的支付是模拟的（直接把状态置为已支付，没有支付渠道也没有流水号）。没有真实收单，就没有可退的钱
     * 2. 
     */
    
}
```
