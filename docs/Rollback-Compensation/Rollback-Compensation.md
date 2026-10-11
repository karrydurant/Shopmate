# 事务回滚补偿

## CouponService.java

整个领券流程：幂等判断-->Redis 预扣-->注册事务回调-->DB 乐观锁扣减-->发放

## CouponMapper.java

乐观锁 SQL：UPDATE ... WHERE version = #{version}
