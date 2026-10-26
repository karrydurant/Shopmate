# Agent 记忆部分

Agent 记忆分为两层——短期记忆+长期记忆

## 短期记忆——short_term

会话级的工作记忆，只服务 "当前这轮对话"，存在 Redis 里，跟着会话 TTL 过期。

职责：

1.将每一轮对话（用户消息 + Agent 回复）追加到 Redis List

2.超出窗口上限时，将最早几轮压缩为摘要

3.返回对话历史时，拼接摘要 + 最近 N 轮

### 写入——append_round

```python
def append_round(self, session_id: str, round_data: ChatRound):
    """追加一轮对话"""
    key=SESSION_PREFIX + session_id
    try:
        self.client.rpush(key, json.dumps(round_data.to_dict(), ensure_ascii=False))
        # 是否要压缩
        self._maybe_trim(session_id, key)
        # 给 key 续期
        self.client.expire(key, settings.session_ttl_seconds)
    except redis.RedisError as e:
        logger.warning("[ShortTermMemory] Redis 不可用，本轮不落短期记忆 | session=%s | %s",
                       session_id, e)
```

### 压缩

#### _maybe_trim

```python
def _maybe_trim(self, session_id: str, key: str):
    size = self.client.llen(key)
    if not size or size <= settings.max_session_rounds:
        return
    
    # 取出最旧的 round_to_compress 轮
    round_to_compress = 5
    old_rounds = []
    for _ in range(round_to_compress):
        raw = self.client.lpop(key)
        if raw:
            old_rounds.append(ChatRound.from_dict(json.loads(raw)))
            
    # 压缩最旧的 round_to_compress 轮
    summary = self._compress_rounds(old_rounds)
    
    # 存入摘要
    summary_key = SUMMARY_PREFIX + session_id
    # 读取会话之前积累的摘要
    existing = self.client.hget(summary_key, "summary") or ""
    # 追加累积——而不是覆盖
    merged = (existing + " " + summary).strip()
    # _cap_summary 先过一遍 _cap_summary 防止 "摘要无限膨胀"
    self.client.hset(summary_key, "summary", self._cap_summary(merged))
    # 给摘要 key 续期，跟会话同生命周期
    self.client.expire(summary_key, settings.session_ttl_seconds)

    logger.info("[ShortTermMemory] compressed %d rounds | session=%s | summary=%s",
                round_to_compress, session_id, summary[:80])
```

#### _cap_summary

把摘要压到字符上限内

```python
def _cap_summary(self, summary: str) -> str:
    limit = settings.memory_summary_max_chars
    if len(summary) <= limit:
        return summary
    tail = summary[-limit:]
    for sep in ("。", "；", "！", "？", "; ", ". "):
        idx = tail.find(sep)
        # 只在切掉的部分不超过三分之一时才归整到句边界
        if 0 <= idx < limit // 3:
            return tail[idx + len(sep):].strip()
        return tail.strip()
```

#### _compress_rounds

将多轮对话压缩成一句话摘要

```python
def _compress_rounds(self, rounds: List[ChatRound]) -> str:
    if not rounds:
        return ""
    
    # 构建对话片段
    dialogue = "\n".join(
        f"{'👤' if r.role == 'user' else '🤖'}: {r.content[:200]}"
        for r in rounds if r.content
    )
    if not dialogue.strip():
        return ""

    # 优先 LLM 摘要
    try:
        return self._llm_summarize(dialogue)
    except Exception as e:
        logger.warning("[ShortTermMemory] LLM summary failed: %s → using keyword fallback", e)
        return self._keyword_summarize(dialogue)
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
