# RAG 知识库与检索

将商品知识（退换货政策、尺码指南、保养说明等）以文档形式存储，
用户提问时检索最相关的知识段落，注入 Agent 的 prompt。

## 整个 RAG 的骨架——RetrieverService

RAG 工作流程：

1.启动时递归加载 docs/ 下所有 Markdown，用 TextChunker 切分为带元数据的 Chunk（heading 按 ## 切；sentence 结构感知 + 尺寸受限 + 重叠）

2.每条 Chunk 转为 KnowledgeEntry，建倒排索引（稀疏臂）+ 向量索引（稠密臂）

3.两路并行召回 → RRF 融合 → Reranker 精排 → Top K

4.任一路失败只损失那一路，另一路照常出结果；两路都失败则返回空

### 数据模型——KnowledgeEntry

```java

```

#### 基础四元组（每条 chunk 必有）

| 字段 | 作用 |
| ---- | ---- |
| doc_id | 全局唯一标识。格式: `来源路径::父小节::子小节`，比如 `after_sale/return_policy.md::退货::七天无理由`。 融合键：两路召回在键空间对齐靠的就是它|
| title | 人读标题，格式是 `文件名 - 小节名` |
| content | 正文内容，切分后的 chunk 文本 |
| source | 源文件相对路径，比如 `after_sale/return_policy.md` |

#### 层级元数据（small-to-big + 评测对齐用）

| 字段 | 作用 |
| ---- | ---- |
| section | 最具体的小节标题。有 `###` 就取 `###`，没有就取 `##`。评测标注和重排器的章节命中都靠它。 |
| parent_section | 父块标题（恒定是 `##`），有 `###` 时它和 section 不同；没有 `###` 时 `parent_section = section` |
| parent_id | 父块的唯一标识（`source::父小节标题`） |
| parent_text | 父块全文。small-to-big 时喂给 LLM 的那一份：命中子块后，向上展开成整节注入。 |
| domain | 知识域，就是 `docs/` 下的一级目录名（after_sale /sizing/care /shipping 等）。后过滤用。 |

#### 缓存与溯源

| 字段 | 作用 |
| ---- | ---- |
| content_hash | 内容指纹（SHA1 前 16 位）。索引缓存的键：本地向量索引按它做差量复用，语料没变就不重算 embedding。刻意不含 source/index —— 文件改名不该触发全量重建。 |

## 切分策略——ChunkerService

知识库文档切分器——把知识库的长文档切成一段段适合检索和喂给大模型的 "知识小块"。切太大，检索精读下降；切太小，上下文不连贯。

两种切分策略：

1.heading 按小节标题切分（粒度粗，上下文完整）：一个 ## 小节就是一块，优点是上下文完整，缺点是粒度粗，长小节里搜一个具体问题匹配不精准

2.sentence 按尺寸切：以 "行" 为单位装块，装到设定大小就停，相邻块留一行重叠，优点是粒度细，词面匹配精准，缺点是长表格/长列表被切开后，上下文可能断

表头继承设计：

问题：用 sentence 策略切长表格的时候，表头只留在第一块，后面几块全是裸数据行

解法：切分的时候先扫描整篇文档，找出所有表格区域，记下表头在哪几行。后面装块的时候，如果这块正好落在表格中段（表头不在这块里），就自动把缺的那几行表头补到块首——但补的表头不计入块大小预算，切分点和没补之前完全一样。

### 按 sentence 策略切（包含表头继承）

```java
List<Chunk> sentenceChunks(List<Section> sections, String source, String stem,
                           Map<String, String> parents) {
    List<Chunk> chunks = new ArrayList<>();
    int idx = 0;
    // 外层循环：遍历每个小节
    // 每个 `##` 或 `###` 小节独立切分，互不影响
    for (Section sec : sections) {
        List<String> units = sec.lines;
        // 找出所有连续 | 开头的行段 [start, end]
        List<int[]> regions = tableRegions(units);
        // 每张表的表头行内容
        List<String> headers = new ArrayList<>();
        List<List<String>> regionHeaders = new ArrayList<>();
        int i = 0, n = units.size();
        while (i < n) {
            if (!units.get(i).startsWith("|")) { i++; continue; }
            int j = i;
            while (j < n && units.get(j).startsWith("|")) j++;
            if (j - i >= 2) {
                int nHead = SEPARATOR_ROW.matcher(units.get(i + 1)).find() ? 2 : 1;
                List<String> h = new ArrayList<>();
                for (int k = i; k < i + nHead; k++) h.add(units.get(k));
                regionHeaders.add(h);
            }
            i = j;
        }

        String[] hf = hierarchyFields(sec, source, parents);
        // 按行装块，装到 chunkSize 就停
        int pos = 0;
        while (pos < units.size()) {
            List<String> buf = new ArrayList<>(); // 当前块的内容
            int end = pos;
            int total = 0;
            // 表头继承：先查这个位置需不需要补表头（表头不计入预算）
            List<String> head = inheritedHeader(units, pos, regions, regionHeaders);
            // 逐行装，直到字符数达到 chunkSize
            while (end < units.size() && total < chunkSize) {
                buf.add(units.get(end));
                total += units.get(end).length();
                end++;
            }
            List<String> combined = new ArrayList<>(head);
            combined.addAll(buf);
            String content = join(combined);
            if (content.length() < 10) {
                pos = end;
                continue;
            }
            // 生成 Chunk，带上完整层级元数据
            Chunk ch = new Chunk(
                    docId(source, sec) + "::c" + idx,
                    stem + " - " + sec.title,
                    sec.title,
                    content,
                    source);
            ch.parentId = hf[0];
            ch.parentSection = hf[1];
            ch.parentText = hf[2];
            chunks.add(ch);
            idx++;
            // 计算下一块的起点
            if (end < units.size()) {
                // overlapUnits 就是重叠部分
                pos += Math.max(1, (end - pos) - overlapUnits);
            } else {
                pos = end;
            }
        }
    }
    return chunks;
}
```

## 多路召回融合——Fusion

## small-to-big 上下文组装——ContextBuilder

## 

## 设计思想

### 为什么要做表头继承

sentence 策略按行装块，长表格会被切成多块，但表头只在第一块里。后面几块全是裸数据行 —— 比如第二块只有 `| L | 96-100 |`，没有 `| 尺码 | 胸围 |`。用户问 "L 对应多少胸围" 时，BM25 匹配到这一块，但块里连 "胸围" 这个词都没有，排序分很低，可能根本召回不到。而且就算召回了，喂给 LLM 的也是一行裸数据，模型不知道 L 是什么尺码、96-100 是什么指标。

### 为什么按 sentence 策略切分时相邻块要留一行重叠呢

切分是机械的，语义是连续的。不做重叠，跨块边界处的信息会被割裂 —— 查询横跨两块的主题时，任何一块都匹配不全。留 1 行重叠，保证相邻块在边界处有交集，检索召回率和上下文连贯性都有提升。

之所以重叠是 "固定一行，不管这一行有多长" 是因为行是这个语料里最小的不可再分单位。如果一行特别长，目前没有这个问题，优化可以改成 "按字符重叠"，前提是能保证不把一条表格记录从中间劈开

### chunkSize=150 是怎么定的？太大太小有什么问题？

太大 → 检索精读下降，一块里混了多个主题，用户问 "退货政策" 可能命中一块里只有一句话讲退货、其他都是尺码表

太小 → 上下文不连贯，一块只有一两行，模型看不懂在说什么

### 为什么按行装块而不是按句子装块？

中文电商知识库的内容主要是 bullet 列表和表格行 —— 比如尺码表、保养说明、退货政策条款。按行装块有两个好处：

1. 不会把一条 bullet 从中间切断

2. 表格行是天然的最小单位 —— 一行就是一个完整数据

### 为什么每个块都带 parentText？只给第一块带不行吗？

### 为什么要有 small-to-big

small-to-big 就是
