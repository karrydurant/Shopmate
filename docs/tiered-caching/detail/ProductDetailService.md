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
                // 有人在回源：已经有一个请求抢先拿到这把 productId 的锁，正在执行 "查 DB、回填缓存"
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
            Thread.currentThread().interrupt();
            // 中断同样不该渲染成「不存在」
            return loadFromDB(productId, false);
        } finally {
            // 用持有者判定
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private static String keyOf(Long productId) {
        return CacheConstants.PRODUCT_DETAIL + productId;
    }

    /**
     * 读 L2 并回填 L1
     *
     * 回填用的是调用方请求的那个 productId，不是反序列化出来的。
     */
    private ProductDetailVO readRedis(Long productId) {
        String json=redisTemplate.opsForValue().get(keyOf(productId));
        if (json==null || CacheConstants.PRODUCT_DETAIL_NULL.equals(json)) {
            return null;
        }
        ProductDetailVO vo=JSON.parseObject(json, ProductDetailVO.class);
        localCache.put(productId, vo);
        return vo;
    }

    // L2里存的是不是负缓存哨兵，是的话顺手回填 L1
    private boolean isNegativeCached(Long productId) {
        String json = redisTemplate.opsForValue().get(keyOf(productId));
        if (CacheConstants.PRODUCT_DETAIL_NULL.equals(json)) {
            localCache.put(productId, NULL_MARKER);
            return true;
        }
        return false;
    }

    // 写入负缓存：本地+Redis各一份
    private void cacheNegative(Long productId) {
        localCache.put(productId, NULL_MARKER);
        redisTemplate.opsForValue().set(keyOf(productId), CacheConstants.PRODUCT_DETAIL_NULL,
                negativeCacheSeconds, TimeUnit.SECONDS);
    }

    // 抢锁失败/被中断的降级路径传 false —— 那一次读是「尽力而为」，不该污染缓存
    private ProductDetailVO loadFromDB(Long productId, boolean writeCache) {
         ProductSpu spu = spuMapper.selectById(productId);
         if (spu == null) {
             if (writeCache) {
                 cacheNegative(productId);
             }
             return null;
        }

        List<ProductSku> skus = skuMapper.selectBySpuId(productId);
        List<ProductDetailVO.SkuItem> skuItems = skus.stream()
                .map(s -> ProductDetailVO.SkuItem.builder()
                        .id(s.getId()).title(s.getTitle())
                        .price(s.getPrice()).originalPrice(s.getOriginalPrice())
                        .stock(s.getStock()).image(s.getImage())
                        .attributes(s.getAttributes()).build())
                .collect(Collectors.toList());

        BigDecimal minPrice = skus.stream().map(ProductSku::getPrice)
                .min(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);
        BigDecimal maxPrice = skus.stream().map(ProductSku::getPrice)
                .max(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);

        ProductDetailVO vo = ProductDetailVO.builder()
                .id(spu.getId()).title(spu.getTitle())
                .categoryId(spu.getCategoryId()).description(spu.getDescription())
                .mainImage(spu.getMainImage()).status(spu.getStatus())
                .minPrice(minPrice).maxPrice(maxPrice)
                .totalSaleCount(skus.stream().mapToInt(ProductSku::getSaleCount).sum())
                .skus(skuItems).build();

        if (writeCache) {
            long ttl = CacheConstants.PRODUCT_DETAIL_TTL + ThreadLocalRandom.current().nextLong(600);
            redisTemplate.opsForValue().set(keyOf(productId), JSON.toJSONString(vo), ttl, TimeUnit.SECONDS);
            localCache.put(productId, vo);
            // 从 DB 学到「这个 id 存在」，立刻补进布隆（兜住刷新窗口内的新商品）
            bloom.markPresent(productId);
            log.info("[ProductDetail] loaded from DB | productId={} | skus={}", productId, skus.size());
        }
        return vo;

        // 失效两级缓存
        public void invalidateCache(Long productId) {
            redisTemplate.delete(keyOf(productId));
            localCache.invalidate(productId);
            log.info("[ProductDetail] cache invalidated | productId={}", productId);
        }

        // 商品新增或更新后调用
        public void onProductUpserted(Long productId) {
            invalidateCache(productId);
            bloom.markPresent(productId);
        }
        // 商品软删后调用
        public void onProductDeleted(Long productId) {
            invalidateCache(productId);
        }
    }
}
```
