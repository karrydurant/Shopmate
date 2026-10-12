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

    // 整个方法一个事务：锁库存、insert订单、insert 订单项，任何一步失败全部回滚
    @Transactional(rollbackFor=Exception.class)
    public Order createOrder(Order order, List<OrderItem> items, String requestId) {
        // 第1步：幂等检查（Redis层）
        // 传了 requestId 才做幂等（没传就是裸调用）
        if (requestId != null && !requestId.isBlank()) {
            // 拼幂等key：order:idempotent:xxx
            String idemKey = "order:idempotent:" + requestId;
            // 查一下这个 requestId 之前是不是已经创建过订单了
            String cachedOrderNo = redisTemplate.opsForValue().get(idemKey);
            // 命中说明这是一次重复请求
            if (cachedOrderNo != null) {
                // 根据缓存里记的  orderNo 把原订单找回来
                Order existing=orderMapper.selectOne(
                        new LambdaQueryWrapper<Order>().eq(Order::getOrderNo, cachedOrderNo));
                // 订单确实还在（防止Redis里有残留但 DB 被清了）
                if (existing != null) {
                    log.info("[Order] idempotent hit | requestId={} | orderNo={}", requestId, cachedOrderNo);
                    return existing;
                }
            }
        }
        // 第二步：幂等检查（DB层兜底）
        // 按 orderNo 再查一次，防止 Redis 挂了或者并发情况下重复落单
        Order existing = orderMapper.selectOne(
                new LambdaQueryWrapper<Order>().eq(Order::getOrderNo, order.getOrderNo()));
        // 已存在就直接返回，防止并发重复落单
        if (existing != null) {
            return existing;
        }

        // 第三步：服务端补全订单价格，不信**客户端**
        BigDecimal total = BigDecimal.ZERO;
        // 遍历每个订单项
        for (OrderItem item : items) {
            // 按 skuId 去 DB 查 SKU
            ProductSku sku = skuMapper.selectById(item.getSkuId());
            // SKU 不存在
            if (sku == null) {
                throw BizException.notFound("SKU 不存在: " + item.getSkuId());
            }
            // 商品名以 DB 里当前的 title 为准，写到订单项里作为"下单快照"
            // 之后商品改名，历史订单里看到的还是当时的名字
            item.setProductName(sku.getTitle());
            item.setPrice(sku.getPrice());
            item.setTotalPrice(sku.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
            total = total.add(item.getTotalPrice());
        }
        // 把算好的总额塞回订单主表
        order.setTotalAmount(total);
        order.setPayAmount(total);

        // 第四步：由应用写 create_time，不用 DB 默认值
        order.setCreateTime(LocalDateTime.now());

        // 第五步：锁库存
        // 对每个订单逐个加锁
        for (OrderItem item:items) {
            // 执行条件 UPDATE：locked_stock += n，WHERE (stock - locked_stock) >= n
            // 数据库行锁会让并发的两条 UPDATE 串行，条件不满足的那一条 affected=0
            int affected = skuMapper.lockStock(item.getSkuId(), item.getQuantity());
            // 锁失败说明可售库存不足
            if (affected==0) {
                // 抛冲突异常，事务回滚，**已锁的其它项靠后面的回调释放**
                throw BizException.conflict("库存不足: " + item.getProductName());
            }
        }

        // 第六步：注册事务回滚钩子
        // 万一后面 insert 订单失败导致事务回滚，需要把刚才锁的库存还会去
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            // 事务结束后回调，status 是提交还是回滚
            @Override
            public void afterCompletion(int status) {
                // 只在回滚时处理
                if (status==STATUS_ROLLED_BACK) {
                    // 遍历所有订单项，逐个释放锁定库存
                    for (OrderItem item : items) {
                        // 执行 releaseStock：locked_stock-=n, WHERE locked_stock>=n
                        //  因为这是回滚后调用，DB 连接已经不在事务里了，直接发新 SQL
                        skuMapper.releaseStock(item.getSkuId(), item.getQuantity());
                    }
                    // 用于发现回滚事件
                    log.warn("[Order] rollback — locked stock released | orderNo={}", order.getOrderNo());
                }
            }
})
        // 第七步：真正落库
        // insert 订单主表
        orderMapper.insert(order);
        // 遍历订单项
        for (OrderItem item:items) {
            // 把订单主键塞回订单项，建立外键关联
            item.setOrderId(order.getId());
            //插入订单项
            orderItemMapper.insert(item);
        }

        // 第八步：写幂等到 Redis
        // 只在成功创建后才写，失败了就不写，让客户端下次还能重试
        if (requestId!=null && !requestId.isBlank()) {
            redisTemplate.opsForValue().setIfAbsent(
                    "order:idempotent:" + requestId, order.getOrderNo(), 24, TimeUnit.HOURS);
        }

        // 打成功日志
        log.info("[Order] created | orderNo={} | userId={} | amount={}",
                order.getOrderNo(), order.getUserId(), order.getPayAmount());
        // 返回创建好的订单（带回填的 id）
        return order;
    }

    // 查某个用户的全部订单，按创建时间倒序
    public List<Order> getUserOrders(String userId) {
        // Lambda 写法：WHERE user_id = ? ORDER BY create_time DESC
        return orderMapper.selectList(
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getUserId, userId)
                        .orderByDesc(Order::getCreateTime));
    }

    // 按 orderNo 查订单，不存在就抛 404
    public Order getByOrderNo(String orderNo) {
        // WHERE order_no = ?
        Order order = orderMapper.selectOne(
                new LambdaQueryWrapper<Order>().eq(Order::getOrderNo, orderNo));
        // 查不到就抛业务异常
        if (order == null) {
            throw BizException.notFound("订单不存在: " + orderNo);
        }
        // 返回
        return order;
    }

    // 通用状态流转方法：状态机校验+CAS更新
    public void transition(Long orderId, OrderStatus to) {
        // 先把订单捞出来看当前状态
        Order order=orderMapper.selectById(orderId);
        // 不存在就抛 404
        if (order == null) {
            throw BizException.notFound("订单不存在: " + orderId);
        }
        // 把 DB 里的 int 状态码转成枚举
        OrderStatus from = OrderStatus.of(order.getStatus());
        // 状态机校验：from -> to 这条边在不在合法转移表里
        OrderStateMachine.requireTransit(from, to);

        // CAS 更新：UPDATE orders SET status=? WHERE id=? AND status=?
        // WHERE 里带 from 状态，并发下只有一个线程能修改成功
        int affected = orderMapper.transitionStatus(orderId, from.getCode(), to.getCode());
        // affected=0 说明在我们校验和更新之间，状态已经被别人改了
        if (affected == 0) {
            // 抛冲突，让调用方重试或刷新
            throw BizException.conflict("订单状态已变更，请刷新后重试");
        }
        // 打日志
        log.info("[Order] status transition | orderId={} | {} -> {}",
                orderId, from.getDesc(), to.getDesc());
    }

    @Transactional(rollbackFor=Exception.class)
    public Order payOrder(String orderNo) {
        // 先把订单捞出来
        Order order=getByOrderNo(orderNo);

        // 第一步：先做状态 CAS，这是并发下的唯一裁决点
        // 同时调支付和取消时，只有一个能把 status 从 0 改成 1 (或5)，另一个在这一步就抛异常
        transition(order.getId(), OrderStatus.PAID);

        // 第二步：把锁的库存真正扣掉。如果这步失败，整个事务回滚，刚才的状态变更也撤销
        // 这样就不会出现 "状态是已支付但库存没扣" 的脏数据
        for (OrderItem item : orderItemsOf(order.getId())) {
            // deductStock：stock -= n, locked_stock -= n，WHERE locked_stock >= n
            int affected=skuMapper.deductStock(item.getSkuId(), item.getQuantity());
            // 扣失败说明锁定账上的数对不上
            if (affected==0) {
                // 抛异常回滚
                throw BizException.conflict("库存锁定已失效，无法支付: " + item.getProductName());
            }
        }

        // 重新查一次，返回最新状态（status 已经是 PAID 了）
        Order paid = getByOrderNo(orderNo);
        // 打日志
        log.info("[Order] paid | orderNo={} | amount={}", orderNo, paid.getPayAmount());
        // 返回
        return paid;
    }

    // 取消订单：状态流转 + 归还锁定库存
    @Transactional(rollbackFor=Exception.class)
    public Order cancelOrder(String orderNo) {
        Order order = getByOrderNo(orderNo);

        // 先做状态 CAS：待支付 -> 已取消
        // 如果用户正在支付，支付先把状态改成了已支付，这里 CAS 失败抛异常
        transition(order.getId(), OrderStatus.CANCELLED);

        // 再把锁定库存还回可售池
        for (OrderItem item : orderItemsOf(order.getId())) {
            // releaseStock: locked_stock-=n, WHERE locked_stock>=n
            int affected = skuMapper.releaseStock(item.getSkuId(), item.getQuantity());
            // 这里和支付侧的处理不一样——只记 warn，不抛异常
            // 原因：如果重复归还，affected=0 说明锁已经不在账上了，这是无害空操作
            // 
        }
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

## 防超卖为什么不用分布式锁

