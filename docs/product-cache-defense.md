# 商品缓存防线

商品详情是电商里读多写少的典型热点，直接打数据库扛不住；但缓存层数一多，就会引出三个经典故障——穿透、击穿、雪崩

## 查商品

### 四级缓存查询主干——ProductDetailService

```java
public class ProductDetailService {

    private final ProductSpuMapper spuMapper;
    private final ProductSkuMapper skuMapper;
    private final StringRedisTemplate redisTemplate;
    private final RedissonClient redissonClient;
    private final ProductIdBloomFilter bloom;

    /** 负缓存存活秒数。短于正缓存：商品随时可能被创建出来。 */
    @Value("${shopmate.product.negative-cache-seconds:60}")
    private long negativeCacheSeconds;

    /** 抢回源锁的最长等待。 */
    @Value("${shopmate.product.lock-wait-ms:1000}")
    private long lockWaitMs;

    /**
     * 锁的持有上限。必须显式给：不传 leaseTime 时 Redisson 会起看门狗给锁续期到
     * 进程结束，一旦某条路径漏了 unlock，那个商品就被永久锁死。
     */
    @Value("${shopmate.product.lock-lease-ms:10000}")
    private long lockLeaseMs;

    private final Cache<Long, ProductDetailVO> localCache = Caffeine.newBuilder()
            .maximumSize(5_000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .recordStats()
            .build();

    /** 本地负缓存哨兵。全 JVM 就这一个实例，所以下面用引用比较判定。 */
    static final ProductDetailVO NULL_MARKER = new ProductDetailVO();

    public ProductDetailVO getProductDetail(Long productId) {
        // L1: Caffeine (含正/负缓存)
        ProductDetailVO local = localCache.getIfPresent(productId);
        // 命中普通对象 → 直接返回
        // 命中 NULL_MARKER → 返回 null
        if (local != null) {
            return local == NULL_MARKER ? null : local;
        }

        // L2: 查 Redis
        // 命中就返回+回填 L1（在 readRedis 里）
        ProductDetailVO fromRedis = readRedis(productId);
        if (fromRedis != null) {
            return fromRedis;
        }
        // 查是不是负缓存
        // readRedis 返回 null 并不意味着商品不存在
        // 如果是负缓存就直接 return null
        // 从来没缓存过就要继续走后面的链路去查
        if (isNegativeCached(productId)) {
            return null;
        }

        // L3: Bloom —— 只有预热成功过才允许用它否决
        if (bloom.isWarmed() && !bloom.mightContain(productId)) {
            log.debug("[ProductDetail] filtered by bloom | productId={}", productId);
            cacheNegative(productId);
            return null;
        }

        // 击穿防护: 分布式锁 + Double-Check
        RLock lock = redissonClient.getLock(CacheConstants.LOCK_PRODUCT_DETAIL + productId);
        boolean locked = false;
        try {
            locked = lock.tryLock(lockWaitMs, lockLeaseMs, TimeUnit.MILLISECONDS);
            if (!locked) {
                // 抢不到锁 = 有人在回源，不是「商品不存在」
                log.warn("[ProductDetail] lock busy, degrading to direct DB read | productId={}", productId);
                // 降级直读 DB，不写缓存
                // DB 是数据源，直接读 DB 读到的就是最新值
                return loadFromDB(productId, false);
            }

            // Double-Check：拿到锁之后在查一次（可能在中间这段时间里 Redis 中数据成功写入了，这时候直接查 Redis）
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
            // 用持有者判定而不是布尔量：中途一旦有别的获取路径，布尔量会漏放或误放
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private static String keyOf(Long productId) {
        return CacheConstants.PRODUCT_DETAIL + productId;
    }

    // 读 L2 并回填 L1。
    private ProductDetailVO readRedis(Long productId) {
        String json = redisTemplate.opsForValue().get(keyOf(productId));
        if (json == null || CacheConstants.PRODUCT_DETAIL_NULL.equals(json)) {
            return null;
        }
        ProductDetailVO vo = JSON.parseObject(json, ProductDetailVO.class);
        localCache.put(productId, vo);
        return vo;
    }

    /** L2 里存的是不是负缓存哨兵。是的话顺手回填 L1，之后连 Redis 都不用问。 */
    private boolean isNegativeCached(Long productId) {
        String json = redisTemplate.opsForValue().get(keyOf(productId));
        if (CacheConstants.PRODUCT_DETAIL_NULL.equals(json)) {
            localCache.put(productId, NULL_MARKER);
            return true;
        }
        return false;
    }

    /** 写入负缓存：本地 + Redis 各一份。 */
    private void cacheNegative(Long productId) {
        localCache.put(productId, NULL_MARKER);
        redisTemplate.opsForValue().set(keyOf(productId), CacheConstants.PRODUCT_DETAIL_NULL,
                negativeCacheSeconds, TimeUnit.SECONDS);
    }

    // 是否把结果写回缓存。抢锁失败/被中断的降级路径传 false —— 那一次读是「尽力而为」，不该污染缓存
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
            // 随机加 0~600s，防雪崩
            long ttl = CacheConstants.PRODUCT_DETAIL_TTL + ThreadLocalRandom.current().nextLong(600);
            redisTemplate.opsForValue().set(keyOf(productId), JSON.toJSONString(vo), ttl, TimeUnit.SECONDS);
            localCache.put(productId, vo);
            // 从 DB 学到「这个 id 存在」，立刻补进布隆（兜住刷新窗口内的新商品）
            bloom.markPresent(productId);
            log.info("[ProductDetail] loaded from DB | productId={} | skus={}", productId, skus.size());
        }
        return vo;
    }

    /**
     * 失效两级缓存。删除商品时也只能做到这一步 —— 布隆过滤器不支持移除元素，
     * 已删的 id 会留在里面继续「可能存在」，读路径回源 DB 拿到 null 后写负缓存兜住。
     */
    public void invalidateCache(Long productId) {
        redisTemplate.delete(keyOf(productId));
        localCache.invalidate(productId);
        log.info("[ProductDetail] cache invalidated | productId={}", productId);
    }

    /**
     * 商品新增或更新后调用（生产者在事务提交后发消息，消费端收到后落到这里）。
     *
     * 比 invalidateCache 多做一件事：把 id 立刻补进布隆。新增的商品如果
     * 只在 DB 里、不在过滤器里，在刷新周期内会被判成「不存在」。
     */
    public void onProductUpserted(Long productId) {
        invalidateCache(productId);
        bloom.markPresent(productId);
    }

    /** 商品软删后调用。见 invalidateCache 里关于布隆无法移除元素的说明。 */
    public void onProductDeleted(Long productId) {
        invalidateCache(productId);
    }
}
```

## 广播（多实例缓存同步）

### MQ 生产者——ProductCacheNotifier

### MQ 消费者——CacheInvalidationConsumer
