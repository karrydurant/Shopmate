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
    private final OrderItemMapper orderItemMapper;
    private final ProductSkuMapper skuMapper;
    // Redis 存下单幂等键
    private final StringRedisTemplate redisTemplate;
    // 无 requestId 的重载入口
    public Order createOrder(Order order, List<OrderItem> items) {
        return createOrder(order, items, null);
    }
    // 整个方法一个事务：下单+锁库存+落库，任何异常都回滚
    @Transactional(rollbackFor=Exception.class)
    // 主创建逻辑：幂等+锁库存+回滚补偿
    public Order createOrder(Order order, List<OrderItem> items, String requestId) {
        // 第0步：requestId 幂等
        // 传了 requestId 才做幂等（没传就是裸调用）
        if (requestId != null && !requestId.isBlank()) {
            // 拼幂等 key
            String idemKey = "order:idempotent:" + requestId;
            // 查 Redis 里这个 requestId 是否已对应一个订单号
            String cachedOrderNo = redisTemplate.opsForValue().get(idemKey);
            // 命中过
            if (cachedOrderNo != null) {
                // 按订单号把订单查出来
                Order existing=orderMapper.selectOne(
                        new LambdaQueryWrapper<Order>().eq(Order::getOrderNo, cachedOrderNo));
                // 订单还在（没被删），直接返回它，不重复下单
                if (existing != null) {
                    log.info("[Order] idempotent hit | requestId={} | orderNo={}", requestId, cachedOrderNo);
                    return existing;
                }
            }
        }
        // 第0.5步：orderNo 查重兜底
        // 按订单号查是否已落库
        Order existing = orderMapper.selectOne(
                new LambdaQueryWrapper<Order>().eq(Order::getOrderNo, order.getOrderNo()));
        // 已存在就直接返回，防止并发重复落单
        if (existing != null) {
            return existing;
        }

        // 第一步：补全订单+算金额
        // 名称和单价一律取 DB 的 SKU 快照，不信客户端传的：
        // 订单要留档"下单那一刻"的价格，商品后来改价不影响历史订单；
        // 金额由服务端算，等于不把定价权交给调用方。
        BigDecimal total = BigDecimal.ZERO;
        // 遍历每个订单项
        for (OrderItem item : items) {
            // 按 SKU id 查 DB
            ProductSku sku = skuMapper.selectById(item.getSkuId());
            // SKU 不存在
            if (sku == null) {
                throw BizException.notFound("SKU 不存在: " + item.getSkuId());
            }
            // 覆盖写入商品名（快照）
            item.setProductName(sku.getTitle());
            // 覆盖写入单价（快照）
            item.setPrice(sku.getPrice());
            // 行小计 = 单价 × 数量
            item.setTotalPrice(sku.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
            // 累加到总额
            total = total.add(item.getTotalPrice());
        }
        // 订单总额
        order.setTotalAmount(total);
        // 优惠券抵扣未实现：目前实付 = 总额，couponCode 仅存档
        order.setPayAmount(total);
    }
}
```

## 防止库存超卖

最后一件商品，100人同时下单。如果不做防护，大家都看到库存有货，都下单成功，卖出 100 件

## 防止重复下单

用户点两次提交/网络超时重试，同一笔请求发两遍。如果不防护，生成两个订单，库存锁两次

## 支付和取消撞车

用户点 "支付" 的同一刻，15分钟超时任务把单取消了。如果不防护，会状态错乱：可能 "取消了却扣了库存"，或 "付了款却库存被还回去"

## 重复支付

支付接口被调用两次（用户连点 / 支付网关重发回调）。如果不防护，会出现同一笔单扣两次库存，或者出现 "已支付→已支付"

## 下单半路崩了：

锁完库存，插订单时 DB 抖动/抛异常

库存被锁住却没人付也没人取消，如果不防护，那 1 件货永久小时（可售-1）

## 重复归还库存

用户手动取消，和超时任务，两个几乎同时把同一单取消。如果不做防护，locked_stock 被减两次变成负数，可售虚高，别人能买到根本没有的货
