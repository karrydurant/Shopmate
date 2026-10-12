# short_term

```python
import json
import logging
from typing import List, Optional

import redis

from agent.config import settings

logger = logging.getLogger(__name__)

SESSION_PREFIX = "agent:session:"
SUMMARY_PREFIX = "agent:session:summary:"

class ChatRound:
    """单轮对话"""
    def __init__(self, role:str, content:str, tool_calls:Optional[list]=None):
        self.role = role
        self.content = content
        self.tool_calls = tool_calls or []

    def to_dict(self) -> dict:
        return {
            "role": self.role,
            "content": self.content,
            "tool_calls": self.tool_calls,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ChatRound":
        return cls(d["role"], d.get("content", ""), d.get("tool_calls", []))

class ShortTermMemory:
    """短期记忆管理器"""

    def __init__(self):
        self.client = redis.from_url(settings.redis_url, decode_responses=True)

    # 写入
    def append_round(self, session_id: str, round_data: ChatRound):
        """追加一轮对话"""
        key=SESSION_PREFIX+session_id
        try:
            self.client.rpush(key, json.dumps(round_data.to_dict(), ensure_ascii=False))
            self._maybe_trim(session_id, key)
            self.client.expire(key, settings.session_ttl_seconds)
        except redis.RedisError as e:
            logger.warning("[ShortTermMemory] Redis 不可用，本轮不落短期记忆 | session=%s | %s",
                           session_id, e)
    def _maybe_trim(self, session_id: str, key: str):
        """超出窗口上限时，将最早几轮压缩为摘要"""
        size=self.client.llen(key)
        if not size or size<=settings.max_session_rounds:
            return
        # 取出最旧的 round_to_compress 轮
        round_to_compress = 5
        old_rounds = []
        for _ in range(round_to_compress):
            raw = self.client.lpop(key)
            if raw:
                old_rounds.append(ChatRound.from_dict(json.loads(raw)))

        # 压缩为摘要（调用 LLM 做单句总结）
        summary = self._compress_rounds(old_rounds)

        # 存入摘要
        summary_key = SUMMARY_PREFIX + session_id
        existing = self.client.hget(summary_key, "summary") or ""
        merged = (existing + " " + summary).strip()
        self.client.hset(summary_key, "summary", self._cap_summary(merged))
        self.client.expire(summary_key, settings.session_ttl_seconds)

        logger.info("[ShortTermMemory] compressed %d rounds | session=%s | summary=%s",
                    round_to_compress, session_id, summary[:80])

    def _cap_summary(self, summary: str)->str:
        """把摘要压到字符上限内"""
        limit=settings.memory_summary_max_chars
        if len()
```

## 定位

短期记忆 -- 多轮对话上下文管理

职责：

1.将每一轮对话（用户消息 + Agent 回复）追加到 Redis List

2.超出窗口上限时，将最早几轮压缩为摘要

3.返回对话历史时，拼接摘要 + 最近 N 轮

Redis 数据结构：
agent:session:{session_id}  List[ChatRound]    完整对话轮次
agent:session:summary:{session_id}   Hash     压缩后的摘要
