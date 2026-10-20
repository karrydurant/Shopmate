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
    private final ProductSpuMapper spuMapper;
    private final ProductSkuMapper skuMapper;
    private final StringRedisTemplate redisTemplate;
    private final RedissonClient redissonClient;
    private final ProductIdBloomFilter bloom;

    // 负缓存存活秒数：短于正缓存：商品随时可能被创建出来
    @Value("${shopmate.product.negative-cache-seconds:60}")
    private long negativeCacheSeconds;

    // 抢回源锁的最长等待
    @Value("${shopmate.product.lock-wait-ms:1000}")
    private long lockWaitMs;

    /**
     * 锁的持有上限。必须显式给：不传 leaseTime 时 Redisson 会起看门狗给锁续期到进程结束
     * 一旦某条路径漏了 unlock，那个商品就被永久锁死
     */
    @Value("${shopmate.product.lock-lease-ms:10000}")
    private long lockLeaseMs;

    // 构建一个 Caffeine 本地内存缓存
    // 最多存 5000 条商品详情
    // 每条数据写入 30 秒后自动过期
    // 开启缓存指标监控
    private final Cache<Long, ProductDetailVO> localCache=Caffeine.newBuilder()
            .maximumSize(5_000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .recordStats()
            .build();

    // 本地负缓存哨兵。全 JVM 就这一个实例，所以下面用引用比较判定
    static final ProductDetailVO NULL_MARKER = new ProductDetailVO ();

    public ProductDetailVO getProductDetail(long productId) {
        // L1: Caffeine (含正/负缓存)
        ProductDetailVO local = localCache.getIfPresent(productId);
        if (local != null) {
            return local == NULL_MARKER ? null : local;
        }

        // L2: Redis
        ProductDetailVO fromRedis = readRedis(productId);
        if (fromRedis != null) {
            return fromRedis;
        }
        if (isNegativeCached(productId)) {
            return null;
        }

        // L3: Bloom —— 只有预热成功过才允许用它否决
        if (bloom.isWarmed() && !bloom.mightContain(productId)) {
            log.debug("[ProductDetail] filtered by bloom | productId={}", productId);
            cacheNegative(productId);
            return null;
        }

        // 击穿防护：分布式锁 + Double-Check
        RLock lock=redissonClient.getLock(CacheConstants.LOCK_PRODUCT_DETAIL + productId);
        boolean locked=false;
        try {
            locked=lock.tryLock(lockWaitMs, lockLeaseMs, TimeUnit.MILLISECONDS);
            if (!locked) {
                // 抢不到锁 = 有人在回源，不是商品不存在。降级直读 DB，不写缓存
                log.warn("[ProductDetail] lock busy, degrading to direct DB read | productId={}", productId);
                return loadFromDB(productId, false);
            }

            // Double-Check Redis：等锁期间别人可能已经回填了
            fromRedis = readRedis(productId);
            if (fromRedis != null) {
                return fromRedis;
            }
            if (isNegativeCached(productId)) {
                return null;
            }
            return loadFromDB(productId, true);
        } catch (InterruptedException e) {
            
        }
    }
}
```

## 缓存穿透 -- 一直查不存在的 id

使用布隆过滤器+负缓存可以解决：布隆启动时把库里所有存在的id用一组哈希塞进一个巨大的位图。它有个性质 -- 说 "不存在" 一定对，说 "存在" 可能错，一些错误 id 可能会被误判为 "存在"。

为什么还要配负缓存：布隆那 1% 误判会把假 id 放进来到 DB，DB 返回 null。这时把 "这 id 不存在" 写个 60 秒短 TTL 的缓存，下次同一个假 id 再来，直接认不存在 —— 补上布隆漏过来的那一小撮。负缓存 TTL 故意短，是因为商品随时可能被新建出来，否定不能存太久。

## 缓存击穿 -- 热点 key 过期那一瞬间被打爆

使用分布式锁+Double-Check 能挡：拿到锁的那个线程去回源DB、回填缓存，其它线程在门口等。等锁的过程中第一个线程已经把 Redis 写好了，所以它们拿到锁后 Double-Check 一次 Redis，发现已经有了，直接返回 -- DB 那一刻只被打一次

抢不到锁不返回 null：这里有个反直觉的设计。抢不到锁 = 别人正在回源，不是商品不存在。如果这时返回 null，前端会渲染成 "404 商品消失了"—— 一次正常的锁竞争被误报成事故。所以代码里降级为直接读一次 DB（多读一次而已，代价远小于误报 404），而且这次不写缓存，免得在没拿到锁的时候抢先回填、破坏锁要保护的那个互斥。

## 缓存雪崩 -- 一批 key 同时过期

使用TTL+随机抖动：过期时间=1小时 + random (0, 600 秒)。本来 "同一秒挂掉 5000 个 key" 被摊成 "接下来 10 分钟陆续挂"，DB 的瞬时尖峰被时间维度摊平了。

## 缓存一致性 -- 脏读

改了 DB 后缓存还是旧值，用户看到旧价格 / 旧库存。

解决办法是先改 DB、事务提交后再删缓存，而且本实例自己删、其他实例靠 RocketMQ 广播删。为什么是 "先改库再删缓存" 而不是反过来 —— 如果先删缓存再改库，中间插进来一个读请求会把旧值重新写回缓存，那删了等于没删。

## 防止错误释放别人的锁 

解锁前用 isHeldByCurrentThread() 判断，不然 finally 里无脑 unlock 会解掉别人刚拿到的锁。

