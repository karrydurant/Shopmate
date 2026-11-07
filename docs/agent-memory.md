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
    // agent:session:{sessionId}
    String key = SESSION_PREFIX + sessionId;
    try {
        // 用 map 存储一条消息，"role": role，"content": content
        Map<String, Object> round = new LinkedHashMap<>();
        round.put("role", role);
        round.put("content", content == null ? "" : content);
        // 右 push 到 Redis List
        // 之所以要用到 key 是因为 Redis 中的存储都是 key-value 格式的
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

maxSessionRounds 是 20，roundsToCompress 是 5

#### capSummary

把摘要压到字符上限内

```java
private String capSummary(String summary) {
    if (summary.length() <= summaryMaxChars) return summary;
    // 在摘要末尾截取 summaryMaxChars 长度
    String tail=summary.substring(summary.length()-summaryMaxChars);
    for (String sep : new String[]{"。", "；", "！", "？"}) {
        int idx=tail.indexOf(sep);
        // 如果第一个出现的标点在句子前面，就认为开头部分是一个被截断的句子，删掉
        if (idx>=0 && idx<summaryMaxChars/3) {
            return tail.substring(idx+sep.length()).strip();
        }
    }
    return tail.strip();
}
```

#### compressRounds

将多轮对话压缩成一句话摘要

```java
private String compressRounds(List<Map<String, Object>> rounds) {
    StringBuilder dialogue=new StringBuilder();
    for (Map<String, Object> r : rounds) {
        String role = String.valueOf(r.getOrDefault("role", ""));
        String content = String.valueOf(r.getOrDefault("content", ""));
        if (!content.isBlank()) {
            String prefix = "user".equals(role) ? "👤: " : "🤖: ";
            // 最多提取 200 字
            dialogue.append(prefix).append(content, 0, Math.min(content.length(), 200)).append('\n');
        }
    }
    String text = dialogue.toString().trim();
    if (text.isEmpty()) return "";

    // 优先 LLM 摘要
    if (llmClient != null) {
        try {
            String llmSummary = llmSummarize(text);
            if (llmSummary != null && !llmSummary.isBlank()) return llmSummary;
        } catch (Exception e) {
            log.warn("[ShortTerm] LLM summary failed → keyword fallback | {}", e.getMessage());
        }
    }
    return keywordSummarize(text);
}
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

```java

```

## 长期记忆

跨会话记住 "这个用户是谁、喜欢什么"

职责：

1.写入——每轮对话做一次 "记忆整理"

2.召回——下一轮对话开头把相关记忆捞出来

### 

## 设计细节

### 为什么要做摘要？摘要保留什么？摘要又是怎么发挥作用的？

随着对话轮次越来越多，每轮都把完整历史塞进 prompt 会导致：

1.Token 成本爆炸

2.上下文窗口撑爆

3.注意力分散——历史太长，LLM 抓不住重点，容易 "忘了" 前面说的偏好

摘要主要是保留早期用户对话中的偏好

通过将摘要拼接进 prompt 发挥作用
