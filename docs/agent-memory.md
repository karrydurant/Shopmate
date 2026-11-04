# Agent 记忆部分

Agent 记忆分为两层——短期记忆+长期记忆

## 短期记忆——ShortTermMemoryService

会话级的工作记忆，只服务 "当前这轮对话"，存在 Redis 里，跟着会话 TTL 过期。

职责：

1.将每一轮对话（用户消息 + Agent 回复）追加到 Redis List

2.超出窗口上限时，将最早几轮压缩为摘要

3.返回对话历史时，拼接摘要 + 最近 N 轮

### 写入——appendRound

```java
public void appendRound(String sessionId, String role, String content) {
    // 如果 sessionId 不存在或为 null 直接返回
    if (sessionId == null || sessionId.isBlank()) return;
    String key = SESSION_PREFIX + sessionId;
    try {
        // 用 map 存储一条消息，"role": role，"content": content
        Map<String, Object> round = new LinkedHashMap<>();
        round.put("role", role);
        round.put("content", content == null ? "" : content);
        // 右 push 到 Redis List
        redis.opsForList().rightPush(key, JsonUtil.toJson(round));
        // 是否要进行压缩
        maybeTrim(sessionId, key);
        // 刷新过期时间
        redis.expire(key, Duration.ofSeconds(sessionTtlSeconds));
    } catch (Exception e) {
        log.warn("[ShortTerm] Redis不可用，本轮不落短期记忆 | session={} | {}", sessionId, e.getMessage());
    }
}
```

### 压缩

#### maybeTrim

```java
private void maybeTrim(String sessionId, String key) {
    Long size = redis.opsForList().size(key);
    if (size == null || size <= maxSessionRounds) return;

    List<Map<String, Object>> oldRounds = new ArrayList<>();
    for (int i = 0; i < roundsToCompress; i++) {
        String raw = redis.opsForList().leftPop(key);
        if (raw == null) break;
        try {
            Map<String, Object> r = JSON.parseObject(raw, new TypeReference<Map<String, Object>>() {});
            oldRounds.add(r);
        } catch (Exception ignored) {
        }
    }
    if (oldRounds.isEmpty()) return;

    String summary = compressRounds(oldRounds);
    appendSummary(sessionId, summary);
    log.info("[ShortTerm] compressed {} rounds | session={}", oldRounds.size(), sessionId);
}
```

#### _cap_summary

把摘要压到字符上限内

```java

```

#### compressRounds

将多轮对话压缩成一句话摘要

```java

```

### 读取

#### get_history

```pyhton
def get_history(self, session_id: str) -> List[ChatRound]:
    key = SESSION_PREFIX + session_id
        try:
            raw_list = self.client.lrange(key, 0, -1)
        except redis.RedisError as e:
            logger.warning("[ShortTermMemory] Redis 不可用，本轮无短期历史 | session=%s | %s",
                           session_id, e)
            return []
        return [ChatRound.from_dict(json.loads(r)) for r in raw_list] if raw_list else []
```

原实现把摘要 包成 `ChatRound("assistant", "[对话摘要] …")` 前置到历史里，有三个问题：

1. 归属错误 —— 模型会认为这段摘要是它自己说过的话

2. 重复注入 —— 摘要已经由 `short_term_context` 走 ContextBuilder 的 `S_SHORT_TERM` 段进 prompt

3. 白吃预算 —— 它作为一条历史消息参与裁剪计量，被裁掉时还会被 `nodes.build_prompt` 的压缩逻辑再总结一次。

#### get_summary

```python
def get_summary(self, session_id: str) -> str:
    """获取会话摘要。"""
    try:
        summary = self.client.hget(SUMMARY_PREFIX + session_id, "summary")
    except redis.RedisError as e:
        logger.warning("[ShortTermMemory] Redis 不可用，本轮无摘要 | session=%s | %s",
                           session_id, e)
        return ""
    return summary if summary else ""
```

## 长期记忆

跨会话记住 "这个用户是谁、喜欢什么"

职责：

1.写入——每轮对话做一次 "记忆整理"

2.召回——下一轮对话开头把相关记忆捞出来

