# 防超卖

## [OrderService.java](detail/OrderService.md)

下单锁库存、支付扣库存、取消归还库存、超时回收

## [ProductSkuMapper.java](detail/ProductSkuMapper.md)

三条带条件的 UPDATE SQL

## [OrderTimeoutScheduler.java](detail/OrderTimeoutScheduler.md)

未支付订单超时自动取消+归还库存
