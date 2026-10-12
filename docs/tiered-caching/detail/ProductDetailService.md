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
    private final StringRedisTemplate redisTemplate;        // L2 Redis
    private final RedissonClient redissonClient;            // 分布式锁（击穿防护）
    private final ProductIdBloomFilter bloom;               // L3 布隆（穿透防护）

    // 负缓存 TTL=60s，短于正缓存：商品随时可能新建
    @Value("${shopmate.product.negative-cache-seconds:60}")
    private long negativeCacheSeconds;                      

    // 抢锁最多等 1s，等不到就降级直读 DB
    @Value("${shopmate.product.lock-wait-ms:1000}")
    private long lockWaitMs;                                

    // 锁持有上限 10s。必须显式传：否则 Redisson 看门狗无限续期，漏 unlock 就永久锁死
    @Value("${shopmate.product.lock-lease-ms:10000}")
    private long lockLeaseMs;
                          
    // L1 Caffeine：最多 5000 条、写后 30s 过期、开启统计（命中率可监控）
    private final Cache<Long, ProductDetailVO> localCache = Caffeine.newBuilder()
            .maximumSize(5_000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .recordStats()
            .build();

    // 本地负缓存哨兵：全 JVM 唯一空对象。判负缓存用引用 == 比较，不用内容比较
    static final ProductDetailVO NULL_MARKER = new ProductDetailVO();

    // 主入口：按 id 取详情，链路 L1 -> L2 -> 布隆 -> 分布式锁 -> L4 DB
    public ProductDetailVO getProductDetail(Long productId) {
        // ① L1：先查本地 Caffeine
        ProductDetailVO local = localCache.getIfPresent(productId);
        if (local != null) {
            return local == NULL_MARKER ? null : local;     // 命中哨兵=不存在，否则直接返回 VO
        }

        // ② L2：本地未中，查 Redis
        ProductDetailVO fromRedis = readRedis(productId);
        if (fromRedis != null) {
            return fromRedis;                               // readRedis 内部已顺手回填 L1
        }
        if (isNegativeCached(productId)) {
            return null;                                    // Redis 里存的是负缓存哨兵，直接判不存在
        }

        // ③ L3：布隆。只有预热成功过才允许它说"不存在"；预热失败一律放行回源
        if (bloom.isWarmed() && !bloom.mightContain(productId)) {
            log.debug("[ProductDetail] filtered by bloom | productId={}", productId);
            cacheNegative(productId);                        // 顺手写负缓存，挡住后续对这个假 id 的穿透
            return null;
        }

        // ④ 击穿防护：热点 key 失效瞬间，只允许一个线程回源 DB
        RLock lock = redissonClient.getLock(CacheConstants.LOCK_PRODUCT_DETAIL + productId);
        boolean locked = false;
        try {
            locked = lock.tryLock(lockWaitMs, lockLeaseMs, TimeUnit.MILLISECONDS);
            if (!locked) {
                // 抢不到锁 = 别人正在回源，不是商品不存在。降级直读 DB，且 writeCache=false 不污染缓存
                log.warn("[ProductDetail] lock busy, degrading to direct DB read | productId={}", productId);
                return loadFromDB(productId, false);
            }

            // Double-Check：等锁期间别人可能已经回填 Redis，再查一次避免重复打 DB
            fromRedis = readRedis(productId);
            if (fromRedis != null) {
                return fromRedis;
            }
            if (isNegativeCached(productId)) {
                return null;                                // 等锁期间别人写了负缓存，直接认
            }
            // 拿到锁了，真正回源 DB，允许写回 L1/L2
            return loadFromDB(productId, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();              // 恢复中断标志（不吞掉）
            return loadFromDB(productId, false);            // 被中断同样降级读 DB，不渲染成"商品不存在"
        } finally {
            // 必须确认锁是自己持有的才能解，否则会误放别人的锁
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    // 拼 Redis key：product:detail:{productId}
    private static String keyOf(Long productId) {
        return CacheConstants.PRODUCT_DETAIL + productId;
    }

    // 读 L2 Redis；命中后顺手回填 L1，下次本机查询不用走网络
    private ProductDetailVO readRedis(Long productId) {
        String json = redisTemplate.opsForValue().get(keyOf(productId));
        if (json == null || CacheConstants.PRODUCT_DETAIL_NULL.equals(json)) {
            return null;                                    // 空值或负缓存哨兵串都视为"没有正数据"
        }
        ProductDetailVO vo = JSON.parseObject(json, ProductDetailVO.class);
        localCache.put(productId, vo);                      // 回填用请求的 productId，不用 vo.getId()：两者若不一致会把 A 商品塞进 B 的缓存
        return vo;
    }

    // 判断 Redis 里存的是不是负缓存哨兵；是的话顺手在 L1 也立哨兵，以后连 Redis 都不用问
    private boolean isNegativeCached(Long productId) {
        String json = redisTemplate.opsForValue().get(keyOf(productId));
        if (CacheConstants.PRODUCT_DETAIL_NULL.equals(json)) {
            localCache.put(productId, NULL_MARKER);
            return true;
        }
        return false;
    }

    // 写负缓存：L1 放哨兵对象、L2 放哨兵串，TTL=negativeCacheSeconds
    private void cacheNegative(Long productId) {
        localCache.put(productId, NULL_MARKER);
        redisTemplate.opsForValue().set(keyOf(productId), CacheConstants.PRODUCT_DETAIL_NULL,
                negativeCacheSeconds, TimeUnit.SECONDS);
    }

    // 真正查 DB 并组装 VO。writeCache=false 表示这次是"尽力而为"的降级读，不许写缓存
    private ProductDetailVO loadFromDB(Long productId, boolean writeCache) {
        ProductSpu spu = spuMapper.selectById(productId);   // 先查 SPU 主档
        if (spu == null) {
            if (writeCache) {
                cacheNegative(productId);                   // DB 真没有：拿到锁的这次写短 TTL 负缓存挡穿透
            }
            return null;
        }

        List<ProductSku> skus = skuMapper.selectBySpuId(productId);  // 再查 SPU 下所有 SKU
        // 把每个 SKU 实体投影成 VO 里的 SkuItem（只带前端要的字段）
        List<ProductDetailVO.SkuItem> skuItems = skus.stream()
                .map(s -> ProductDetailVO.SkuItem.builder()
                        .id(s.getId()).title(s.getTitle())
                        .price(s.getPrice()).originalPrice(s.getOriginalPrice())
                        .stock(s.getStock()).image(s.getImage())
                        .attributes(s.getAttributes()).build())
                .collect(Collectors.toList());

        // 价格区间：从 SKU 列表取 min/max
        BigDecimal minPrice = skus.stream().map(ProductSku::getPrice)
                .min(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);
        BigDecimal maxPrice = skus.stream().map(ProductSku::getPrice)
                .max(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);

        // 组装最终 VO：SPU 字段 + SKU 列表 + 价格区间 + 总销量
        ProductDetailVO vo = ProductDetailVO.builder()
                .id(spu.getId()).title(spu.getTitle())
                .categoryId(spu.getCategoryId()).description(spu.getDescription())
                .mainImage(spu.getMainImage()).status(spu.getStatus())
                .minPrice(minPrice).maxPrice(maxPrice)
                .totalSaleCount(skus.stream().mapToInt(ProductSku::getSaleCount).sum())
                .skus(skuItems).build();

        // 只有拿到锁的那次回源才写缓存：TTL = 1h + 0~600s 随机抖动，防雪崩
        if (writeCache) {
            long ttl = CacheConstants.PRODUCT_DETAIL_TTL + ThreadLocalRandom.current().nextLong(600);
            redisTemplate.opsForValue().set(keyOf(productId), JSON.toJSONString(vo), ttl, TimeUnit.SECONDS);
            localCache.put(productId, vo);
            bloom.markPresent(productId);                   // DB 都说有这个 id 了，立刻补进布隆，兜住刷新窗口内的新 id
            log.info("[ProductDetail] loaded from DB | productId={} | skus={}", productId, skus.size());
        }
        return vo;
    }

    // 失效 L2 Redis + L1 Caffeine。布隆删不了元素，已删 id 留在里面也无害，
    // 读路径回源 DB 拿到 null 后会写负缓存兜住
    public void invalidateCache(Long productId) {
        redisTemplate.delete(keyOf(productId));
        localCache.invalidate(productId);
        log.info("[ProductDetail] cache invalidated | productId={}", productId);
    }

    // 新增/更新后调用：清缓存 + 把 id 立刻补进布隆。
    // 不补的话新商品在下次布隆全量刷新（默认 5 分钟）前会被误判成"不存在"
    public void onProductUpserted(Long productId) {
        invalidateCache(productId);
        bloom.markPresent(productId);
    }

    // 软删后调用：只清缓存。布隆删不了元素，留着它"可能存在"即可
    public void onProductDeleted(Long productId) {
        invalidateCache(productId);
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

