# 订单与库存账本

## OrderService

主要负责订单服务——库存锁定+订单状态机

核心方法：下单、支付、取消、退款、发起退货、仓库确认收到货、状态流转

### createOrder——下单

```java
// 创建订单——幂等+库存锁定
// 幂等保证：
// 1. requestId 幂等键：相同 requestId 只创建一次，重复请求返回已有订单
// 2. orderNo 查重：orders 表 uk_order_no 唯一索引兜底，防止重复落单
@Transactional(rollbackFor = Exception.class)
public Order createOrder(Order order, List<OrderItem> items, String requestId) {
    if (order.getUserId() == null || order.getUserId().isBlank()) {
        throw BizException.badParam("用户不能为空");
    }
    // 入参校验不能只靠控制器的 Bean Validation：服务层也可能被直接调用
    // 而负数数量在下面的金额计算与 lockStock 里都不会报错
    // SQL 侧另有 `n > 0` 兜底
    if (items == null || items.isEmpty()) {
        throw BizException.badParam("订单项不能为空");
    }
    for (OrderItem item : items) {
        if (item == null || item.getSkuId() == null || item.getQuantity() == null || item.getQuantity() <= 0) {
            throw BizException.badParam("SKU 与购买数量不能为空，且数量必须大于 0");
        }
    }

    // 幂等
    String idemKey=null;
    if (requestId != null && !requestId.isBlank()) {
        idemKey = idempotencyKey(order.getUserId(), requestId);
        // 先查库：Redis 重启/过期 时，唯一索引(MySQL)仍然保证不会落两单
        Order existingByRequest = orderMapper.selectOne(new LambdaQueryWrapper<Order>()
                .eq(Order::getUserId, order.getUserId())
                .eq(Order::getRequestId, requestId));
        if (existingByRequest != null) {
            log.info("[Order] idempotent hit by requestId | requestId={} | orderNo={}", requestId, existingByRequest.getOrderNo());
            return existingByRequest;
        }
        Boolean firstRequest;
        // 在 Redis 里抢占一个占位键
        try {
            firstRequest = redisTemplate.opsForValue()
                    .setIfAbsent(idemKey, IDEMPOTENCY_PENDING,
                            IDEMPOTENCY_TTL_HOURS, TimeUnit.HOURS);
        } catch (Exception e) {
            // Redis 不可用时不让下单整体失败：继续走，DB 唯一索引兜底。
            log.error("[Order] idempotency reservation failed — fall back to DB unique index | requestId={}",
                    requestId, e);
            firstRequest = null;
        }
        if (Boolean.FALSE.equals(firstRequest)) {
            // reolay: 把上一次请求的结果查回来重放给这次请求
            Order replay = replayFromIdempotencyKey(idemKey, order.getUserId(), requestId);
            if (replay != null) {
                return replay;
            }
            throw BizException.conflict("相同 requestId 的请求正在处理中，请稍后重试");
        }

        order.setRequestId(requestId);
        registerIdempotencyFinalizer(idemKey, order.getOrderNo());

        // 幂等：orderNo 已存在则直接返回（防止重复落单），orderNo 是订单号
        // 这里主要是防止服务层被直接调用时传入重复单号的情况
        Order existing = orderMapper.selectOne(
                new LambdaQueryWrapper<Order>().eq(Order::getOrderNo, order.getOrderNo()));
        if (existing != null) {
            if (!Objects.equals(existing.getUserId(), order.getUserId())) {
                throw BizException.conflict("订单号已被其他用户占用");
            }
            return existing;
        }

        // 补全订单项+计算金额。名称与单价一律取 DB 的商品快照，不接受客户端传入
        BigDecimal total = BigDecimal.ZERO;
        for (OrderItem item : items) {
            ProductSku sku = skuMapper.selectById(item.getSkuId());
            if (sku == null) {
                throw BizException.notFound("SKU 不存在: " + item.getSkuId());
            }
            item.setProductName(sku.getTitle());
            item.setPrice(sku.getPrice());
            item.setTotalPrice(sku.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
            total = total.add(item.getTotalPrice());
        }
        order.setTotalAmount(total);

        // 用券抵扣。校验（门槛 / 有效期 / 券型 / 归属）与核销都在 applyToOrder 里完成
        BigDecimal discount = couponService.applyToOrder(
                order.getUserId(), order.getCouponCode(), total, order.getOrderNo());
        order.setDiscountAmount(discount);
        order.setPayAmount(total.subtract(discount));

        order.setCreateTime(LocalDateTime.now());

        // 锁定库存（防超卖）
        for (OrderItem item : items) {
            int affected = skuMapper.lockStock(item.getSkuId(), item.getQuantity());
            // 为什么 affected==0 代表库存不足：SQL语句
            if (affected == 0) {
                throw BizException.conflict("库存不足: " + item.getProductName());
            }
        }

        // 落订单 + 订单项
        orderMapper.insert(order);
        for (OrderItem item : items) {
            item.setOrderId(order.getId());
            item.setId(IdWorker.getId());
        }
        // 购物车 k 件从 k 次往返降到 1 次
        orderItemMapper.insertBatch(items);

        // 5. 无事务环境（单测直接调用）下补写幂等键；有事务时由 afterCommit 写。
        //    afterCommit 保证键指向的订单已经真的提交，回滚时则删键。
        if (idemKey != null && !TransactionSynchronizationManager.isSynchronizationActive()) {
            writeIdempotencyKey(idemKey, order.getOrderNo());
        }

        log.info("[Order] created | orderNo={} | userId={} | amount={}",
                order.getOrderNo(), order.getUserId(), order.getPayAmount());
        return order;
    }
}
```

负数校验：

创建订单有两层幂等保证：

requestId 和 orderNo 如果 requestId 重复了就直接返回上次请求的结果，orderNo 主要预防服务层被直接调用

补全订单项+计算金额

落订单与订单项

### transition——状态流转

```java
// 状态 CAS + 审计落库
// 审计行与状态变更在同一个事务里：状态改了而日志没写，等于这条流转没有发生过
public void transition(Long orderId, OrderStatus to, String operator) {
    Order order = orderMapper.selectById(orderId);
    if (order == null) {
        throw BizException.notFound("订单不存在: " + orderId);
    }
    OrderStatus from = OrderStatus.of(order.getStatus());
    OrderStateMachine.requireTransit(from, to);
    int affected = orderMapper.transitionStatus(orderId, from.getCode(), to.getCode());
    if (affected == 0) {
        throw BizException.conflict("订单状态已变更，请刷新后重试");
    }

    OrderStatusLog logRow = new OrderStatusLog();
    logRow.setOrderId(orderId);
    logRow.setFromStatus(from.getCode());
    logRow.setToStatus(to.getCode());
    logRow.setOperator(operator == null || operator.isBlank() ? OPERATOR_SYSTEM : operator);
    logRow.setCreatedAt(LocalDateTime.now());
    statusLogMapper.insert(logRow);

    log.info("[Order] status transition | orderId={} | {} -> {} | operator={}",
            orderId, from.getDesc(), to.getDesc(), logRow.getOperator());
}
```

### payOrder——支付订单

```java
/**
 * 支付订单——状态流转与库存扣减在同一个事务里
 *
 * 为什么先 CAS 状态再动库存
 * 顺序是刻意的：
 * 1.CAS 是并发下的唯一裁决点。同一笔订单同时被支付和取消时，保证只有一个能改状态。
 *   如果是先改库存，发现状态变了还要回滚撤销
 * 2.扣减失败必须连状态一起撤销。抛异常让事务回滚，于是"标记为已支付但库存没扣"这种订单在本实现里不存在。
 */
@Transactional(rollbackFor=Exception.class)
public Order payOrder(String orderNo) {
    Order order=getByOrderNo(orderNo);

    // 先裁决：只有"待支付->已支付"能过，并发下唯一
    transition(order.getId(), OrderStatus.PAID, OPERATOR_USER);

    // 再把锁定库存转为实际扣减（失败即抛异常，状态变更随之回滚）
    for (OrderItem item : orderItemsOf(order.getId())) {
        int affected = skuMapper.deductStock(item.getSkuId(), item.getQuantity());
        if (affected == 0) {
            throw BizException.conflict("库存锁定已失效，无法支付: " + item.getProductName());
        }
    }

    Order paid=getByOrderNo(orderNo);
    log.info("[Order] paid | orderNo={} | amount={}", orderNo, paid.getPayAmount());
    return paid;
}
```

### cancelOrder——取消订单

```java
// 取消订单——状态流转与锁定库存归还在同一个事务里。
@Transactional(rollbackFor=Exception.class)
public Order cancelOrder(String orderNo, String operator) {
    Order order=getByOrderNo(orderNo);

    transition(order.getId(), OrderStatus.CANCELLED, operator);

    // 退券
    couponService.returnForOrder(order.getCouponCode(), orderNo);

    for (OrderItem item : orderItemsOf(order.getId())) {
        int affected = skuMapper.releaseStock(item.getSkuId(), item.getQuantity());
        if (affected == 0) {
            // 与支付侧的扣减失败处理相反——这里只记 warn
            log.warn("[Order] release affected 0 rows | orderNo={} | skuId={} | qty={}",
                    orderNo, item.getSkuId(), item.getQuantity());
        }
    }

    Order cancelled = getByOrderNo(orderNo);
    log.info("[Order] cancelled | orderNo={} | locked stock returned", orderNo);
    return cancelled;
}
```

### refundOrder——退款

```java

```
