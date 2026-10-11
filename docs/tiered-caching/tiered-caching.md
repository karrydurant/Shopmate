# 多级缓存

四级缓存纵深防御：Caffeine、Redis、BloomFilter、DB

```mermaid
flowchart TB
    %% ============ 读路径 ============
    subgraph READ["📖 读路径 GET /api/products/{id}/detail"]
        Start([请求 productId]) --> L1{L1: Caffeine<br/>本地 30s}
        L1 -->|命中正缓存| Ret1([返回商品详情])
        L1 -->|命中 NULL_MARKER| RetNull1([返回 null])
        L1 -->|未命中| L2{L2: Redis<br/>1h + 0~600s 随机TTL}

        L2 -->|命中正缓存| FillL1[回填 L1 Caffeine] --> Ret1
        L2 -->|命中负哨兵 __SHOPMATE_NULL__| FillNull[回填 L1 NULL_MARKER] --> RetNull1
        L2 -->|未命中| Bloom{L3: BloomFilter<br/>isWarmed 且 !mightContain?}

        Bloom -->|预热成功 且 判为不存在| Neg1[写负缓存<br/>本地 + Redis 各一份 60s] --> RetNull1
        Bloom -->|预热失败 / 可能存在| Lock{Redisson 分布式锁<br/>tryLock 1s 等 / 10s 持有}

        Lock -->|抢不到锁| Degrade[降级: 直读 DB<br/>不写缓存] --> RetDB
        Lock -->|被中断| Degrade
        Lock -->|抢到锁| DC{Double-Check<br/>再查 Redis}
        DC -->|命中| Ret1
        DC -->|负缓存| RetNull1
        DC -->|仍未命中| DB[(L4: MySQL<br/>spu + skuMapper)]

        DB -->|spu == null| Neg2[写负缓存] --> RetNull2([返回 null])
        DB -->|查到| Assemble[组装 ProductDetailVO] --> WriteBack[写回 L1 + L2<br/>随机 TTL + 补布隆 markPresent] --> Unlock[释放锁]
        WriteBack --> RetDB([返回商品详情])
    end

    %% ============ 写路径 ============
    subgraph WRITE["✏️ 写路径 /api/admin/products (增/改/删)"]
        WStart([Admin 写请求]) --> Tx[开启 @Transactional]
        Tx --> DBOps[写 product_spu / product_sku]
        DBOps --> Commit{事务提交?}
        Commit -->|afterCommit| LocalInv[本实例立即失效:<br/>清 Redis + 清本地 Caffeine<br/>INSERT/UPDATE 补布隆]
        LocalInv --> Notify[ProductCacheNotifier<br/>syncSend topic=product_change]
        Notify -.->|MQ 不可用则仅记日志<br/>业务不回滚, 等 TTL 自愈| MQDown[(RocketMQ)]
        Notify --> MQUp[(RocketMQ)]
    end

    MQUp -.->|BROADCASTING 广播<br/>每个实例都收一份| Consumer[CacheInvalidationConsumer<br/>实例级 dedupKey]
    Consumer --> OnUp{op 类型?}
    OnUp -->|INSERT / UPDATE| Upserted[onProductUpserted<br/>清缓存 + markPresent]
    OnUp -->|DELETE| Deleted[onProductDeleted<br/>只清缓存<br/>布隆无法移除, 由负缓存兜]

    style READ fill:#e3f2fd,stroke:#1976d2
    style WRITE fill:#fff3e0,stroke:#f57c00
    style DB fill:#fce4ec,stroke:#c2185b
    style MQUp fill:#f3e5f5,stroke:#7b1fa2
    style Degrade fill:#fff9c4,stroke:#f9a825
    style Neg1 fill:#ffccbc,stroke:#d84315
    style Neg2 fill:#ffccbc,stroke:#d84315
```

## ProductDetailService.java

## ProductIdBloomFilter.java

## ProductWriteService.java

## ProductCacheNotifier.java

## CacheInvalidationConsumer.java

## CacheConstants.java

## IdempotentConsumer.java
