# OrderService

```java
package com.shopmate.order.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.shopmate.common.exception.BizException;
import com.shopmate.order.model.entity.Order;
import com.shopmate.order.model.entity.OrderItem;
import com.shopmate.order.model.enums.OrderStatus;
import com.shopmate.order.repository.OrderItemMapper;
import com.shopmate.order.repository.OrderMapper;
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
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderMapper orderMapper;
    private final OrderMapper orderItemMapper;
    private final ProductSkuMapper skuMapper;
    private final StringRedisTemplate redisTemplate;

    public Order createOrder(Order order, List<OrderItem> items) {
        return createOrder(order, items, null);
    }

    @Transactional(rollbackFor=Exception.class)
    public Order createOrder(Order order, List<OrderItem> items, String requestId) {
        // 幂等：相同 requestId 只创建一次，重复请求返回已有订单
        if (requestId!=null && !requestId.isBlank()) {
            String idemKey="order:idempotent:"+
        }
    }
}
```
