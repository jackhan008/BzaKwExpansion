# Expansion Pipeline 技术文档

> 最后更新：2026-03-30

---

## 目录

1. [系统概览](#1-系统概览)
2. [完整处理流程](#2-完整处理流程)
3. [各模块详解](#3-各模块详解)
   - [3.1 AI 扩展 (ai_expander.py)](#31-ai-扩展-ai_expanderpy)
   - [3.2 查询匹配 (matcher.py)](#32-查询匹配-matcherpy)
   - [3.3 数据库管理 (db_manager.py)](#33-数据库管理-db_managerpy)
   - [3.4 Web 服务 (app.py)](#34-web-服务-apppy)
   - [3.5 主流程 (main.py)](#35-主流程-mainpy)
4. [性能瓶颈分析](#4-性能瓶颈分析)
5. [优化方案](#5-优化方案)
   - [Quick Wins（低改动、高收益）](#51-quick-wins低改动高收益)
   - [中期优化](#52-中期优化)
   - [长期架构优化](#53-长期架构优化)
6. [优化优先级汇总](#6-优化优先级汇总)

---

## 1. 系统概览

```
输入: Search Theme（品牌名）
  ↓
[AI 扩展] → 生成 5-15 个相关关键词（Azure OpenAI）
  ↓
[匹配阶段] → 对每个关键词做 Hard Match + Vector Match
  │  Hard Match: SQLite LIKE 查询（精确子串匹配）
  │  Vector Match: FAISS 语义相似度搜索
  ↓
[AI 验证] → 批量校验匹配结果是否真正属于该品牌（Azure OpenAI）
  ↓
输出: 带评分的 CSV（normalized_query, Relevance, Score, AI_Valid, ...）
```

**关键数字：**

| 指标 | 值 |
|------|----|
| 每个 Theme 最多生成关键词 | 15 个 |
| Vector Search Top-K | 100 |
| Similarity 阈值 | > 0.8 |
| Relevance 过滤阈值 | ≥ 0.4 |
| 验证批次大小 | 10 queries / batch |
| 验证并发线程数 | 5 |
| max_completion_tokens（扩展） | 8000 |
| max_completion_tokens（验证） | 8000 |

---

## 2. 完整处理流程

```
process_theme(theme, expander, matcher, market)
│
├─ Step 1: AI Expansion
│   └─ AIExpander.expand_search_theme(theme, market)
│       ├─ _get_language_instruction(market) → 市场语言提示词
│       ├─ Azure OpenAI API Call (1次，同步)
│       │   └─ model: gpt-5-mini | max_tokens: 8000
│       ├─ 解析 JSON 数组
│       └─ [Japan] _expand_japanese_space_variants() → 全/半角空格变体
│           └─ 返回: List[str]，e.g. ["nike shoes", "ナイキ", ...]
│
├─ Step 2: Matching
│   └─ QueryMatcher.process_expanded_keywords(expanded_keywords)
│       │
│       ├─ For each keyword（顺序循环）:
│       │   │
│       │   ├─ [Hard Match] db.query_sqlite_contains(keyword)
│       │   │   ├─ SQL: SELECT ... WHERE REPLACE(query,' ','') LIKE '%{term}%'
│       │   │   ├─ Score: 2
│       │   │   └─ Relevance: len(keyword) / max(len(query), len(keyword))
│       │   │
│       │   └─ [Vector Match] db.query_vector_similarity(keyword, n=100)
│       │       ├─ 生成 query embedding（SentenceTransformer）
│       │       ├─ FAISS IndexFlatIP.search(embedding, 100)
│       │       ├─ 过滤: similarity > 0.8
│       │       ├─ 过滤: keyword in query OR levenshtein < len(keyword)/5
│       │       ├─ Score: 1
│       │       └─ Relevance: cosine similarity
│       │
│       ├─ 合并所有关键词的匹配结果到 dict（key = normalized_query）
│       ├─ Score = score_hard + score_vector（最大 3）
│       ├─ 过滤: Relevance < 0.4
│       └─ 返回: DataFrame，列 = [normalized_query, Relevance, SRPV, AdClick, revenue, Score, matched_keyword]
│
└─ Step 3: AI Validation
    └─ AIExpander.validate_queries(theme, queries, market)
        ├─ 将 queries 切分为每批 10 个
        ├─ ThreadPoolExecutor(max_workers=5) 并发调用
        │   └─ _validate_batch(brand, batch, batch_index, market)
        │       ├─ Azure OpenAI API Call（每批 1 次）
        │       └─ 返回: {query: {is_valid: bool, reason: str}}
        └─ 合并所有批次结果，写入 DataFrame 的 AI_Valid / AI_Reason 列
```

---

## 3. 各模块详解

### 3.1 AI 扩展 (ai_expander.py)

#### `expand_search_theme(theme, market)`

| 属性 | 值 |
|------|----|
| API 调用次数 | 1 次（同步，阻塞） |
| 输出 tokens 上限 | 8000 |
| 典型响应 tokens | ~200-500 |
| 出错回退 | 返回 `[theme]` 原词 |

**Prompt 结构：**
```
[System Prompt]
  - 市场语言要求（_get_language_instruction）
  - 最多生成 15 个关键词
  - 不含竞品品牌 / 对比词 / 无关实体
  - 必须体现品牌意图

[User Input]
  User Input: {theme}
```

**日本市场特殊处理：**
- 对生成的所有关键词，自动添加全角空格（U+3000）版本
- 例：`"ナイキ シューズ"` → 同时保留 `"ナイキ　シューズ"` (全角)

---

#### `validate_queries(brand, queries, market)` + `_validate_batch(...)`

| 属性 | 值 |
|------|----|
| 批次大小 | 10 queries |
| 并发线程 | 5 |
| API 调用次数 | ceil(n/10) 次 |
| 出错回退 | 全部标记为 is_valid=True |

**验证规则（Prompt 中）：**
1. 品牌名称精确匹配 / 变体 / 拼写错误 / URL 变体 / 本地语言 → ✅ Valid
2. 父品牌组合（如 `耐克 nike`）→ ✅ Valid
3. 与竞品对比（如 `nike vs adidas`）→ ❌ Invalid
4. 无关实体组合（如 `nike pizza`）→ ❌ Invalid
5. 通用词歧义（如品牌同名通用词）→ 谨慎判断

---

### 3.2 查询匹配 (matcher.py)

#### `process_expanded_keywords(expanded_keywords)`

**Hard Match 流程：**
```python
df = db.query_sqlite_contains(keyword)
# SQL: WHERE REPLACE(REPLACE(query,' ',''), '　','') LIKE '%{clean_term}%'
relevance = len(keyword_clean) / max(len(query_clean), len(keyword_clean))
score_hard = 2
```

**Vector Match 流程：**
```python
df = db.query_vector_similarity(keyword, n_results=100)
# 过滤条件（同时满足之一）:
# 1. similarity > 0.8 AND keyword in query（子串包含）
# 2. similarity > 0.8 AND levenshtein(query, keyword) < len(keyword)/5
score_vector = 1
```

**最终评分：**
```
Score = score_hard (0 or 2) + score_vector (0 or 1)
可能值: 1, 2, 3（实际不存在 0，因为未命中的 query 不会进入 all_results）
  Score=1 → 只被 Vector Match 命中（纯语义匹配）
  Score=2 → 只被 Hard Match 命中（精确子串匹配）
  Score=3 → Hard Match + Vector Match 都命中（最高置信度）
过滤: Relevance < 0.4 的记录被丢弃（代码 matcher.py 第 129 行）
```

**Relevance 说明：**
- Hard Match 时：`Relevance = len(keyword去空格) / max(len(query去空格), len(keyword去空格))`
  - 含义：keyword 占 query 长度的比例，keyword 越短/query 越长，值越低
  - 例：keyword="nike"(4), query="nike running shoes"(15去空格) → 4/15 ≈ 0.27
  - 例：keyword="nike air max"(9), query="nikeairmax"(9) → 9/9 = 1.0
- Vector Match 时：`Relevance = cosine_similarity`（FAISS 返回值，已过滤 > 0.8）
- 最终取两者最大值（`relevance_accum = max(...)`）

---

### 3.3 数据库管理 (db_manager.py)

#### SQLite (`query_sqlite_contains`)

```sql
-- 标准市场
SELECT normalized_query, SRPV, AdClick, revenue
FROM keywords
WHERE REPLACE(normalized_query, ' ', '') LIKE '%{clean_term}%'

-- 日本市场（额外去除全角空格）
SELECT normalized_query, SRPV, AdClick, revenue
FROM keywords
WHERE REPLACE(REPLACE(normalized_query, ' ', ''), '　', '') LIKE '%{clean_term}%'
```

已建索引：`idx_query ON keywords(normalized_query)`、`idx_id ON keywords(id)`

> ⚠️ `LIKE '%term%'` 为中缀匹配，**无法使用前缀索引**，每次均为全表扫描。

#### FAISS (`query_vector_similarity`)

```python
# 索引类型: IndexFlatIP（精确内积搜索）
# 向量已 L2 归一化 → 内积 = 余弦相似度
index.search(query_embedding, 100)  # 返回 top-100
# 返回 distance = 1 - similarity
```

---

### 3.4 Web 服务 (app.py)

| 端点 | 模式 | 说明 |
|------|------|------|
| `POST /api/expand` | 同步 | 等所有 theme 处理完后返回 |
| `POST /api/expand_stream` | 流式 NDJSON | 每个 theme 完成即推送，最后推送完整 CSV |

**流式处理核心：**
```python
# 在线程池中运行同步函数，避免阻塞事件循环
result = await loop.run_in_executor(None, process_theme, theme, expander, matcher, market)
yield json.dumps({"type": "theme_result", "data": ...}) + "\n"
```

**DBManager 缓存：**
```python
db_managers = {}  # market → DBManager（全局缓存，避免重复加载 FAISS）
```

---

### 3.5 主流程 (main.py)

```python
def process_theme(theme, expander, matcher, market):
    # 1. 扩展
    expanded_keywords = expander.expand_search_theme(theme, market)
    # 2. 匹配
    results_df = matcher.process_expanded_keywords(expanded_keywords)
    # 3. 验证
    queries = results_df['normalized_query'].unique().tolist()
    validation = expander.validate_queries(theme, queries, market)
    # 4. 写回
    results_df['AI_Valid'] = results_df['normalized_query'].map(...)
    results_df['AI_Reason'] = results_df['normalized_query'].map(...)
    return results_df, expanded_keywords
```

---

## 4. 性能瓶颈分析

### 时间分布估算（单个 Theme，AU 市场，~500 匹配结果）

| 步骤 | 耗时估算 | 占比 | 瓶颈类型 |
|------|---------|------|---------|
| AI Expansion（1次 API） | 3–8s | ~15% | **网络 I/O** |
| SQLite Hard Match（每个关键词 1 次） | 0.5–3s（×15 关键词） | ~20% | **全表扫描** |
| Embedding 生成（每个关键词 1 次） | 0.1–0.5s（×15） | ~10% | **CPU/Model** |
| FAISS 精确搜索（×15） | 0.1–1s（×15） | ~10% | **内存带宽** |
| AI Validation（500 queries → 50 批 × 5 并发） | 10–40s | **~45%** | **网络 I/O** |
| Pandas 操作 | <0.5s | ~2% | 可忽略 |

> **验证阶段是最大瓶颈**，约占总耗时的 45%。

### 详细瓶颈列表

#### 🔴 严重瓶颈

**1. AI 验证 – 串行主题 + 大量 API 调用**
- 500 个匹配 queries → 50 个批次 → 约 10 次并发 API 轮次
- 多个 theme 之间是串行处理的（流式接口的限制）
- 每次 API 调用 3–10s

**2. SQLite LIKE 全表扫描**
- `LIKE '%term%'` 无法走前缀索引
- 每个 expanded keyword 单独一次查询（顺序执行）
- Japan 数据库 92M 行，扫描代价极高

#### 🟠 中等瓶颈

**3. FAISS IndexFlatIP – 精确穷举搜索**
- O(n) 复杂度：每次搜索都遍历全部向量
- Japan: 114M 行 × 384 维 float32 ≈ 165GB 内存（实际 index 3.7GB 已压缩）
- 每个关键词独立调用 embedding model（无批处理）

**4. Validation 批次过小 + 线程数保守**
- 批次 10 queries，可提升至 20–30
- 5 个线程对于高并发 API 偏保守

**5. 验证不过滤低分结果**
- Score=0 或 Relevance 极低的 query 也进入验证队列
- 浪费 API token

#### 🟡 轻微瓶颈

**6. 多个 theme 串行处理（非流式接口）**
- `/api/expand` 端点逐个处理 theme，没有并发

**7. max_completion_tokens=8000 设置过高**
- 扩展实际只用 ~200–500 tokens
- 验证每批实际约 500–2000 tokens
- 不影响速度，但影响成本

---

## 5. 优化方案

### 5.1 Quick Wins（低改动、高收益）

#### ✅ 优化1：多 Theme 并发处理（已实现）

**涉及文件：** `main.py`（新增 `process_themes_parallel`）、`app.py`（两个端点均改用并发）

**原因：** 原来多个 theme 是完全串行的：theme1 的 Expand+Match+Validate 全部跑完，才开始 theme2。每个 theme 的验证阶段动辄 10–40s，串行完全浪费了等待时间。

**并发架构：**
```
原来（串行）:
Theme1: [Expand]→[Match]→[Validate 10s]
Theme2:                               [Expand]→[Match]→[Validate 10s]
Theme3:                                                               [Expand]→[Match]→[Validate 10s]
总耗时: ~30s

现在（并发，max_workers=3）:
Theme1: [Expand]→[Match]→[Validate 10s]
Theme2: [Expand]→[Match]→[Validate 10s]   ← 同时进行
Theme3: [Expand]→[Match]→[Validate 10s]   ← 同时进行
总耗时: ~10s（约 3x 加速）
```

**并发层次（两层嵌套）：**
```
process_themes_parallel(max_workers=3)       ← 外层：theme 级并发
  └─ process_theme(theme_1)
       └─ validate_queries(max_workers=10)   ← 内层：batch 级并发（已有）
  └─ process_theme(theme_2)
       └─ validate_queries(max_workers=10)
  └─ process_theme(theme_3)
       └─ validate_queries(max_workers=10)
```
峰值最多同时发起 3×10 = **30 个并发 API 请求**，需确认 Azure OpenAI RPM 限制。

**max_workers 参数参考：**
| max_workers | 峰值并发 API 请求 | 适用场景 |
|-------------|-----------------|---------|
| 1 | 10 | 保守/低配额 |
| 3（默认） | 30 | 推荐起点 |
| 5 | 50 | 高配额时使用 |

---

#### ✅ 优化2：扩大验证批次大小 + 增加并发线程（已实现）

**位置：** `ai_expander.py` → `validate_queries()`

```python
# 改动：validate_queries 内部并发线程数 5 → 10
MAX_WORKERS = 10
```

批次大小维持 10（足够，单批次 token 量可控），并发数翻倍后每个 theme 内部的验证吞吐量提升约 2x。

**注意：** 结合外层 3 个 theme 并发，内层 10 线程，峰值 30 个并发请求，需确认 Azure OpenAI 速率限制。

---

#### ✅ 优化3：多个关键词批量 Embedding

**位置：** `db_manager.py` → `query_vector_similarity()`

```python
# 当前：每个关键词单独 encode
embedding = self.embedding_model.encode([term])

# 优化后：在 matcher.py 中批量 encode 所有关键词
embeddings = self.db.embedding_model.encode(
    expanded_keywords,
    batch_size=32,
    show_progress_bar=False
)
# 然后对每个 embedding 做 FAISS 搜索
```

**预期收益：** Embedding 生成时间减少 40–70%（batch encode 比循环快得多）。

---

#### ✅ 优化4：降低 max_completion_tokens

**位置：** `config.py`

```python
# 当前
MAX_COMPLETION_TOKENS_EXPAND = 8000
MAX_COMPLETION_TOKENS_VALIDATE = 8000

# 优化后（节省成本，不影响正确性）
MAX_COMPLETION_TOKENS_EXPAND = 1000   # 15 个关键词最多 ~500 tokens
MAX_COMPLETION_TOKENS_VALIDATE = 3000  # 20 个 query 约 1000–2000 tokens
```

**预期收益：** 降低 API 成本，对某些模型可略微降低延迟。

---

#### ✅ 优化5：Expansion 结果缓存

**位置：** `ai_expander.py` 或 `app.py`

```python
import functools

# 简单内存缓存（同一 session 内）
_expand_cache = {}

def expand_search_theme(self, theme, market="Australia"):
    cache_key = (theme.lower().strip(), market)
    if cache_key in _expand_cache:
        return _expand_cache[cache_key]
    result = ...  # 原有逻辑
    _expand_cache[cache_key] = result
    return result
```

**预期收益：** 重复处理相同品牌时节省 100% 的 AI 扩展调用。

---

### 5.2 中期优化

#### 🔧 优化6：SQLite → 全文搜索索引（FTS5）

**位置：** `db_manager.py` → `initialize_db()` + `query_sqlite_contains()`

```python
# 建表时额外创建 FTS5 虚拟表
cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS keywords_fts
    USING fts5(normalized_query, content='keywords', content_rowid='id')
""")
cursor.execute("INSERT INTO keywords_fts(keywords_fts) VALUES('rebuild')")

# 查询时使用 FTS MATCH 而非 LIKE
cursor.execute("""
    SELECT k.normalized_query, k.SRPV, k.AdClick, k.revenue
    FROM keywords k
    JOIN keywords_fts fts ON k.id = fts.rowid
    WHERE keywords_fts MATCH ?
""", (f'"{term}"',))
```

**预期收益：** 中缀查询速度提升 5–50x（取决于数据量和词项分布）。

> ⚠️ FTS5 对中文/日文/泰文等非空格分词语言需要额外配置 tokenizer（如 `unicode61` 或自定义）。

---

#### 🔧 优化7：FAISS 精确索引 → HNSW 近似索引

**位置：** `db_manager.py` → `initialize_db()`

```python
# 当前：精确搜索 O(n)
index = faiss.IndexFlatIP(dimension)

# 优化后：HNSW 近似搜索 O(log n)
index = faiss.IndexHNSWFlat(dimension, 32)  # 32 = M 参数（连接数）
index.hnsw.efConstruction = 200             # 构建质量
index.hnsw.efSearch = 64                    # 搜索质量
```

**对比：**

| 指标 | IndexFlatIP | IndexHNSWFlat |
|------|-------------|---------------|
| 搜索速度 | O(n) | O(log n) |
| Recall@100 | 100% | ~95-99% |
| 内存占用 | 基准 | +50%（存图结构） |
| 适用场景 | <1M 向量 | >1M 向量 |

> ⚠️ HNSW 索引不支持动态删除，仅适合静态数据集（符合本项目场景）。

---

#### 🔧 优化8：多 Theme 并行处理（非流式接口）

**位置：** `app.py` → `POST /api/expand`

```python
# 当前：串行
for theme in themes:
    result = process_theme(theme, ...)

# 优化后：并行（非流式接口）
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {
        executor.submit(process_theme, theme, expander, matcher, market): theme
        for theme in themes
    }
    for future in as_completed(futures):
        result_df, keywords = future.result()
        ...
```

**预期收益：** 多 theme 场景下时间从 O(n) → O(n/3)，约 3x 加速。

---

#### 🔧 优化9：SQLite 批量查询多个关键词

**位置：** `db_manager.py` + `matcher.py`

```python
# 当前：每个关键词单独一次 SQL 查询
for keyword in expanded_keywords:
    df = db.query_sqlite_contains(keyword)

# 优化后：一次 SQL 查询所有关键词（用 OR 连接）
def query_sqlite_contains_batch(self, terms: list) -> pd.DataFrame:
    clean_terms = [t.replace(' ', '').replace('\u3000', '') for t in terms]
    conditions = " OR ".join(
        [f"REPLACE(normalized_query,' ','') LIKE '%{t}%'" for t in clean_terms]
    )
    sql = f"SELECT normalized_query, SRPV, AdClick, revenue FROM keywords WHERE {conditions}"
    return pd.read_sql_query(sql, conn)
```

**预期收益：** SQLite 查询次数从 N 次减少到 1 次，减少连接开销。

> ⚠️ OR 连接多条件时需注意 SQL 注入风险，应使用参数化查询或白名单过滤。

---

### 5.3 长期架构优化

#### 🏗️ 优化10：异步化 AI API 调用

将 `ai_expander.py` 中的同步 OpenAI 调用改为异步：

```python
from openai import AsyncAzureOpenAI

class AIExpander:
    def __init__(self):
        self.client = AsyncAzureOpenAI(...)

    async def expand_search_theme_async(self, theme, market):
        response = await self.client.chat.completions.create(...)
        ...

    async def validate_queries_async(self, brand, queries, market):
        tasks = [self._validate_batch_async(brand, batch, i, market)
                 for i, batch in enumerate(batches)]
        results = await asyncio.gather(*tasks)
        ...
```

**配合 app.py 的流式接口**，可以真正实现非阻塞并发处理，而不是用 `run_in_executor` 把同步代码跑在线程池里。

---

#### 🏗️ 优化11：预计算 Expansion 结果（离线缓存）

对于固定的品牌词库，可以预先运行一遍扩展并将结果持久化：

```python
# 预计算并保存
expansion_cache = {}
for theme in brand_list:
    expansion_cache[theme] = expander.expand_search_theme(theme, market)

with open("expansion_cache.json", "w") as f:
    json.dump(expansion_cache, f, ensure_ascii=False)
```

**运行时直接读取缓存，跳过 AI 扩展步骤。**

---

#### 🏗️ 优化12：验证结果持久化缓存

将验证结果存入 SQLite，避免对相同 query 重复验证：

```python
# 建立验证缓存表
CREATE TABLE IF NOT EXISTS validation_cache (
    brand TEXT,
    query TEXT,
    market TEXT,
    is_valid INTEGER,
    reason TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (brand, query, market)
);

# 查询前先查缓存
SELECT is_valid, reason FROM validation_cache
WHERE brand=? AND query=? AND market=?
```

---

## 6. 优化优先级汇总

| 优先级 | 优化项 | 改动量 | 预期提速 | 推荐指数 |
|--------|--------|--------|---------|---------|
| 🥇 P1 | 验证前过滤低质量结果 | 小 | 30–60% 减少 API 调用 | ⭐⭐⭐⭐⭐ |
| 🥇 P1 | 扩大验证批次 (10→20) + 并发 (5→10) | 小 | 2–4x 验证吞吐量 | ⭐⭐⭐⭐⭐ |
| 🥇 P1 | 批量 Embedding（替代逐个 encode） | 小 | 40–70% embedding 加速 | ⭐⭐⭐⭐⭐ |
| 🥈 P2 | 降低 max_completion_tokens | 极小 | 节省成本，略降延迟 | ⭐⭐⭐⭐ |
| 🥈 P2 | Expansion 结果内存缓存 | 小 | 重复品牌 100% 节省 | ⭐⭐⭐⭐ |
| 🥈 P2 | 多 Theme 并行（非流式接口） | 中 | ~3x（多 theme 场景） | ⭐⭐⭐⭐ |
| 🥉 P3 | SQLite → FTS5 全文搜索 | 中 | 5–50x SQL 查询加速 | ⭐⭐⭐ |
| 🥉 P3 | FAISS FlatIP → HNSW | 中 | 大数据集下显著加速 | ⭐⭐⭐ |
| 🏅 P4 | 全异步化 AI 调用 | 大 | 最优并发，架构更优 | ⭐⭐ |
| 🏅 P4 | 验证结果持久化缓存 | 大 | 长期运营成本下降 | ⭐⭐ |

---

*文档基于代码版本：2026-03-30*
