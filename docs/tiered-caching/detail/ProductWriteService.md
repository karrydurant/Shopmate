# ProductWriteService

最重要的是下面几行代码
```java
TransactionSynchronizationManager.registerSynchronization()
```

写路径遵循 Cache-Aside：先写库，事务提交后再删缓存。提交后先同步清本实例的 Caffeine、Redis 和布隆，再通过 RocketMQ 广播通知其他实例清各自本地缓存。用广播而不是集群消费，是因为本地缓存是每个进程堆内私有的，必须每台机器都收到。一致性上接受最终一致，不追求强一致 ——MQ 挂了就靠 TTL 自愈，不反过来阻塞业务写。
