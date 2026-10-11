# ProductDetailService

```java
package com.shopmate.product.service;

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.shopmate.common.constant.CacheConstants;
import com.shopmate.product.model.entity.ProductSku;
import com.shopmate.product.model.entity.ProductSpu;
import com.shopmate.product.model.vo.ProductDetailVO;
import com.shopmate.product.repository.ProductSkuMapper;
import com.shopmate.product.repository.ProductSpuMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor

public class ProductDetailService {
    public ProductDetailVO getProductDetail(Long productId) {
        // 第一步：查L1本地缓存
        ProductDetailVO local=localCache.getIfPresent(productId);
        if (local != null) {
            // 命中了，区分是真商品还是"不存在"的哨兵
            return local==NULL_MARKER ? null : local;
        }
        // 第二步：查 L2 Redis
        ProductDetailVO fromRedis=readRedis(productId);
        if (fromRedis!=null) {
            return fromRedis;  // readRedis 内部已经顺手把数据回填到 L1 了
        }
        // Redis 里是不是存着"这个 id 不存在"的哨兵串
        if (isNegativeCached(productId)) {
            return null;
        }
        // 第三步：查 L3 布隆过滤器
        // 两个条件都要满足才能否决：
        // 1.bloom.isWarmed()=true(预热成功过，布隆才有发言权)
        // 2.!bloom.mightContain(productId)：布隆说这个 id 根本不可能存在
        // 预热失败时直接放行到 DB -- 否则启动时 DB 抖一下，全站商品都被判成不存在
        if (bloom.isWarmed() && !bloom.mightContain(productId)) {
            log.debug("[ProductDetail] filtered by bloom | productId={}", productId);
            cacheNegative(productId);
            return null;
        }
        // 第四步：都没命中，要回源 DB 了。先抢分布式锁防击穿
        // 锁的key按商品 id 隔离：不同商品互不影响
        RLock lock=redissonClient.getLock(lockWaitMs, lockLeaseMs, TimeUnit.MILLISECONDS);
        if (!locked) {
            // 没抢到锁=别人正在回源，不是"商品不存在"
            // 直接读一次 DB（只读，不写缓存），把数据给用户。
            log.warn("[ProductDetail] lock busy, degrading to direct DB read | productId={}", productId);
            return loadFromDB(productId, false);
        }
        // Double-Check：拿到锁之后再查一次 Redis
        // 因为你等锁的这1秒里，别人可能已经查完 DB 把缓存填好了
        // 不复查就会重查一次 DB
        fromRedis=readRedis(productId);
        if (fromRedis!=null) {
            return fromRedis;
        }
        if (isNegativeCached(productId)) {
            return null;
        }
        //真的回源 DB，这次允许写缓存
        return loadFromDB(productId, true);
    } catch (InterruptedException e) {
        // 等锁时被中断了。恢复中断标记，然后照样降级直读 DB——
        // 中断也不能让用户看到"商品不存在"
        Thread.currentThread().interrupt();
        return loadFromDB(productId, false);
    } finally {
        // 释放锁，但要判断
        // locked=true 我们才拿到过锁
        // isHeldByCurrentThread()：锁可能已经因为 lease 超时自动释放，又被别人抢走了。直接 unlock 会解别人的锁，是 bug。
        if (locked && lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }
    //拼 Redis key："product:detail:123"
    private static String keyOf(Long productId) {
        return CacheConstants.PRODUCT_DETAIL + productId;
    }
    //从 Redis 读商品详情，命中就回填 L1
    
}
```
