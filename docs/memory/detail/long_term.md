# long_term

```python
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from agent.config import settings

from agent.memory.factory import build_memory_store, memory_config

from agent.memory.recall import AccessLedger, decay_factor, rank_by_tier

from agent.memory.store import (
    DEFAULT_SECTION, SOURCE_CONVERSATION, STATUS_ACTIVE, TYPE_PREFERENCE,
    MemoryRecord, new_pk,
)

from agent.slots import SLOT_KEYS, SlotDelta

logger = logging.getLogger(__name__)

_TIER_OVERFETCH=3


```

