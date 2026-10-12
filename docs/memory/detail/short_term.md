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
