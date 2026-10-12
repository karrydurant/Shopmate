# 事务回滚补偿

## [CouponService.java](detail/CouponService.md)

整个领券流程：幂等判断-->Redis 预扣-->注册事务回调-->DB 乐观锁扣减-->发放

## CouponMapper.java

```java
package com.shopmate.coupon.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.shopmate.coupon.model.entity.Coupon;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface CouponMapper extends BaseMapper<Coupon> {
    @Update("UPDATE coupon SET remaining_count = remaining_count - 1, version = version + 1 " +
            "WHERE coupon_code = #{couponCode} AND remaining_count > 0 AND version = #{version}")
    int decrementRemainingCount(@Param("couponCode") String couponCode, @Param("version") int version);
    @Update("UPDATE coupon SET remaining_count = remaining_count + 1, version = version + 1 " +
            "WHERE coupon_code = #{couponCode} AND version = #{version}")
    int incrementRemainingCount(@Param("couponCode") String couponCode, @Param("version") int version);
}
```
