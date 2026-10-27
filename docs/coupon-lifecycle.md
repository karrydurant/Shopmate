# 优惠券生命周期

## 优惠券全生命周期——CouponService

### 领券——acquireCoupon

用户点领取时走这个方法

```java
@Transactional(rollbackFor = Exception.class)
public boolean acquireCoupon(String userId, String couponCode) {
    // 业务幂等：一人限领一张，已领取直接拒绝
    Long already = userCouponMapper.selectCount(
            new LambdaQueryWrapper<UserCoupon>()
                    .eq(UserCoupon::getUserId, userId)
                    .eq(UserCoupon::getCouponCode, couponCode));
    if (already != null && already > 0) {
        throw BizException.conflict("您已领取过该优惠券");
    }

    // 并发去重：短窗口幂等键，避免同用户同券并发重复发券
    String idemKey = "coupon:acquire:" + userId + ":" + couponCode;
    Boolean firstRequest = redisTemplate.opsForValue()
            .setIfAbsent(idemKey, "1", 10, TimeUnit.SECONDS);
    if (Boolean.FALSE.equals(firstRequest)) {
        throw BizException.conflict("操作过于频繁，请稍后再试");
    }

    // 读券：DB 是库存权威来源。这一步既做存在性 / 有效期校验，又给下一步的回源取初值
    Coupon coupon = couponMapper.selectOne(
            new LambdaQueryWrapper<Coupon>().eq(Coupon::getCouponCode, couponCode));
    if (coupon == null || coupon.getRemainingCount() <= 0) {
        throw BizException.notFound("优惠券不存在或已过期");
    }
    if (coupon.getEndTime() != null && coupon.getEndTime().isBefore(LocalDateTime.now())) {
        throw BizException.badParam("优惠券已过期");
    }

    // 回源：Redis 库存键不存在时按 DB 建它 —— 必须在预扣之前
    seedStockFromDbIfAbsent(couponCode, coupon.getRemainingCount());

    // Redis 预扣库存
    String stockKey = STOCK_KEY_PREFIX + couponCode;
    Long remain = redisTemplate.opsForValue().decrement(stockKey);
    if (remain == null || remain < 0) {
        redisTemplate.opsForValue().increment(stockKey);
        throw BizException.conflict("优惠券已抢光");
    }

    // 注册事务回调：无论何种原因回滚，都恢复 Redis 库存 + 释放幂等键
    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
        @Override
        public void afterCompletion(int status) {
            if (status == STATUS_ROLLED_BACK) {
                redisTemplate.opsForValue().increment(stockKey);
                redisTemplate.delete(idemKey);
                log.warn("[Coupon] rollback — Redis stock restored | code={}", couponCode);
            }
        }
    });

    // DB 乐观锁扣减（复用第 1 步读到的 version，不重复查库）
    int affected = couponMapper.decrementRemainingCount(couponCode, coupon.getVersion());
    if (affected == 0) {
        throw BizException.conflict("优惠券库存不足，请稍后再试");
    }

    // 发放到用户账户
    userCouponMapper.issueToUser(userId, couponCode);

    log.info("[Coupon] acquired | userId={} | code={}", userId, couponCode);
    return true;
}
```

短窗口幂等键：

用户快速点了两次 "领取" 按钮，发了两个请求。如果不拦着，两个请求都走完流程，用户就领到了两张券。

怎么实现短窗口的：`.setIfAbsent(idemKey, "1", 10, TimeUnit.SECONDS)` 幂等键 10 s后过期。10 s后用户再点就是新请求

之所以不是建一个永久键：1.每个用户每个券都建一个永久键，Redis 里会存海量的键；2.落库后不再需要它了，DB 里就有记录，
后面再来请求，查 user_coupon 表就会拦住 ——"您已领取过该优惠券"；3.永久键会误伤正常请求，假设用户领过券了，过了一会儿又点进来看看 
—— 如果 Redis 键是永久的，他会看到 "操作过于频繁"，但其实他是想查一下自己有没有领到，不是想再领一次。短窗口过期后，这个提示就不会再出现了。




### 回源建 Redis 库存

### 下单用券——applyToOrder

### 退券——returnForOrder


## 库存对账——CouponStockReconcileScheduler

优惠券库存对账定时任务 —— 每 10 分钟扫一遍，把 Redis 里的预扣库存数和 DB 里的实际库存数对齐

```java
public class CouponStockReconcileScheduler {

    private static final String STOCK_KEY_PREFIX = "coupon:stock:";
    private static final int BATCH_SIZE = 500;

    private static final DefaultRedisScript<Long> CAS_SET = new DefaultRedisScript<>(
            // 对账的时候从 Redis 读了一个值，现在我准备把它改成 DB 的数
            // 改之前再查一次 Redis 里的值有没有变
            // 如果变了，证明中间有人动过了
            "local current = redis.call('GET', KEYS[1]) " +
                    "if current == ARGV[1] then " +
                    "  redis.call('SET', KEYS[1], ARGV[2]) " +
                    "  return 1 " +
                    "end " +
                    "return 0",
            Long.class);

    private final CouponMapper couponMapper;
    private final StringRedisTemplate redisTemplate;
    private final ScheduledJobLease jobLease;

    /** 上一次扫描里观察到的偏差，key = couponCode，value = "redis值->db值"。 */
    private final Map<String, String> pendingDrift = new ConcurrentHashMap<>();

    /** 分页游标；一次扫不完时从上次停下的 id 继续。 */
    private long cursor = 0;

    @Scheduled(fixedDelayString = "${shopmate.coupon.stock-reconcile-ms:600000}")

    // 入口，每 10 分钟触发一次，但用 `ScheduledJobLease` 租约保证多实例只有一个真在跑
    public void reconcile() {
        jobLease.tryRun("coupon-stock-reconcile", 600_000, this::reconcileLeased);
    }

    private void reconcileLeased() {
        // 一次扫 500 张，游标推进，扫完一轮重置游标
        List<Coupon> batch = couponMapper.selectActivePageForReconcile(
                cursor, LocalDateTime.now(), BATCH_SIZE);

        if (batch.isEmpty()) {
            cursor = 0;
            return;
        }

        int seeded = 0;
        int repaired = 0;
        int deferred = 0;
        int casLost = 0;
        for (Coupon coupon : batch) {
            try {
                switch (reconcileOne(coupon)) {
                    case SEEDED -> seeded++;
                    case REPAIRED -> repaired++;
                    case DEFERRED -> deferred++;
                    case CAS_LOST -> casLost++;
                    case IN_SYNC -> { }
                }
            } catch (Exception e) {
                // 单张券的 Redis 故障不影响同批其他券；下一轮再试。
                log.warn("[Coupon] stock reconcile failed | code={}", coupon.getCouponCode(), e);
            }
        }

        cursor = batch.size() < BATCH_SIZE ? 0 : batch.get(batch.size() - 1).getId();
        if (seeded + repaired + deferred + casLost > 0) {
            log.info("[Coupon] stock reconcile | scanned={} | seeded={} | repaired={} "
                            + "| deferred={} | casLost={}",
                    batch.size(), seeded, repaired, deferred, casLost);
        }
    }

    // 对每张券，对比 Redis 值和 DB 值，有 5 种结果
    private Outcome reconcileOne(Coupon coupon) {
        String code = coupon.getCouponCode();
        Integer dbCount = coupon.getRemainingCount();
        if (code == null || code.isBlank() || dbCount == null || dbCount < 0) {
            return Outcome.IN_SYNC;
        }

        String key = STOCK_KEY_PREFIX + code;
        String raw = redisTemplate.opsForValue().get(key);

        // Redis 键不存在
        if (raw == null) {
            // 键不存在，没有需要保护的状态：与领券路径同一手法，SETNX 播种即可。
            Boolean seeded = redisTemplate.opsForValue()
                    .setIfAbsent(key, String.valueOf(dbCount));
            if (Boolean.TRUE.equals(seeded)) {
                log.info("[Coupon] stock key re-seeded | code={} | remaining={}", code, dbCount);
                return Outcome.SEEDED;
            }
            // 有人刚刚播种 / 领券：本轮不动，下一轮重新观测。
            return Outcome.CAS_LOST;
        }

        long redisCount;
        try {
            redisCount = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            log.warn("[Coupon] corrupt stock key | code={} | value={}", code, raw);
            // 值坏了，当成一次确定的偏差处理。
            return repair(key, code, raw, dbCount);
        }

        // Redis 值和 DB 值一样
        if (redisCount == dbCount) {
            pendingDrift.remove(code);
            return Outcome.IN_SYNC;
        }

        // 第一次看到偏差
        String signature = raw + "->" + dbCount;
        String previous = pendingDrift.put(code, signature);
        if (!signature.equals(previous)) {
            // 之所以不立即修
            // 因为领券是 "先减 Redis、后提交 DB"，对账刚好夹在两步中间读到的，看起来像偏差，其实事务马上就提交、两边自然相等
            log.warn("[Coupon] stock drift observed, will repair if it persists "
                            + "| code={} | redis={} | db={}",
                    code, redisCount, dbCount);
            return Outcome.DEFERRED;
        }

        // 连续两轮看到同一偏差 → 真偏差，修
        Outcome outcome = repair(key, code, raw, dbCount);
        if (outcome == Outcome.REPAIRED) {
            pendingDrift.remove(code);
        }
        return outcome;
    }

    private Outcome repair(String key, String code, String observed, int dbCount) {
        Long changed = redisTemplate.execute(
                CAS_SET, List.of(key), observed, String.valueOf(dbCount));
        if (Long.valueOf(1L).equals(changed)) {
            log.info("[Coupon] stock repaired | code={} | redis:{} -> {}",
                    code, observed, dbCount);
            return Outcome.REPAIRED;
        }
        // 观测之后又有领券 / 其他人修过：以最新值为准，下一轮重新观测。
        log.info("[Coupon] stock repair skipped, value changed since observation | code={}", code);
        return Outcome.CAS_LOST;
    }

    private enum Outcome {
        IN_SYNC,
        SEEDED,
        REPAIRED,
        DEFERRED,
        CAS_LOST
    }
}
```

为什么要对账？

Redis 的 coupon:stock: 是领券路径上的前置防线，DB 的 remaining_count 是权威值。两边在正常路径上每发一张各减一次、回滚时 
Redis 由事务回调补回；但进程在"Redis 已扣、DB 未提交"之间崩溃时，补偿不会执行，于是 Redis 低于 DB —— 用户看到"已抢光"，而券池里其实还有。
反方向（Redis 高于 DB）不会超发（DB 的乐观锁是最终裁决），但会让请求白白撞一次"库存不足"。

修正 repair 用 Lua CAS，而不是直接 SET？

因为对账期间可能又有人领券扣了 Redis。如果直接 SET 成 DB 值，就把别人刚扣的额度又还回去了 —— 一份库存卖出两次。

Lua CAS 的逻辑是："当前值还等于我刚才读到的值时，才改成 DB 值"。如果中间有人改过了，脚本什么都不做，留给下一轮再扫。

为什么连续两次看到偏差才修改？

领券是"先减 Redis、后提交 DB"。对账若在两者之间读到值，看到的是 "Redis 已减、DB 未减"，这看起来像 Redis 低于 DB，其实交易马上就会提交、两边自然
相等。每轮扫描相隔分钟级，在途事务远短于此，所以只有连续两轮读到同一偏差才认定为真偏差。这样避免了把在途预扣当成漂移修回去（那会让 Redis 短暂多出一张，
最终由 DB 拒绝，不会超发，但没有必要制造这次往返）。
