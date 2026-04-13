# BZA Keywords Expand — 项目执行流程文档

## 概览

本项目是一个品牌广告关键词扩展工具，用于将用户输入的「搜索主题」（品牌或产品名）扩展为一批相关的搜索关键词，并从市场数据库中匹配出真实用户查询，最终输出可用于广告投放的关键词列表（CSV）。

---

## 系统架构

```
用户 (浏览器)
    │
    ▼
app.py  (FastAPI 服务, port 7888)
    │
    ├── /api/expand_stream  (SSE 流式响应)
    └── /api/expand         (批量同步响应)
            │
            ▼
        main.py: process_themes_parallel()
            │  ThreadPoolExecutor (max_workers=3)
            │
            ▼ (每个 theme 并行执行)
        main.py: process_theme()
            │
            ├── Step 1: ai_expander.py — AIExpander.expand_search_theme()
            ├── Step 2: matcher.py    — QueryMatcher.process_expanded_keywords()
            └── Step 3: ai_expander.py — AIExpander.validate_queries()
```

---

## 核心执行流程（单个 Theme）

### Step 1 — AI 关键词扩展 (`ai_expander.py`)

- 输入：搜索主题（如 `"Nike"`）+ 目标市场（如 `"Australia"`）
- 调用 **Azure OpenAI** (GPT 模型)，按市场语言生成最多 15 个扩展关键词
- 语言规则：英语市场只生成英文；多语言市场（日本、中国等）同时生成本地语言 + 英文变体
- 日本市场特殊处理：自动为每个关键词生成全角空格（`　`）和半角空格（` `）两个版本
- 输出：关键词列表，如 `["nike", "nike shoes", "nike air max", ...]`

### Step 2 — 关键词匹配 (`matcher.py` + `db_manager.py`)

对每个扩展关键词，同时执行两种匹配方式：

| 方式 | 方法 | 得分 | 逻辑 |
|------|------|------|------|
| **Hard Match** | SQLite LIKE 查询 | Score +2 | 去除空格后，数据库查询包含该关键词的所有记录 |
| **Vector Match** | FAISS 向量相似度 | Score +1 | embedding 余弦相似度 ≥ 0.8，且满足包含关系或 Levenshtein 编辑距离阈值 |

**Relevance（相关度）** 计算：
- Hard Match：`len(keyword) / max(len(query), len(keyword))`
- Vector Match：余弦相似度值
- 取多次匹配中的最大值，过滤 Relevance < 0.4 的结果

**数据库结构（每个市场独立）**：
- `keywords_<market>.db` — SQLite，存储原始查询数据（`normalized_query`, `SRPV`, `AdClick`, `revenue`）
- `keywords_<market>.index` — FAISS 向量索引，初始化时由 CSV 文件生成

**Embedding 模型**：
- 英语市场（Australia、Philippines）：`all-MiniLM-L6-v2`
- 多语言市场：`paraphrase-multilingual-MiniLM-L12-v2`

### Step 3 — AI 验证 (`ai_expander.py`)

- 对匹配到的所有唯一查询，以每批 10 条的方式，并行调用 Azure OpenAI 进行验证
- 最多 10 个并发验证批次（ThreadPoolExecutor）
- 验证规则（6 条）：
  1. 必须代表目标品牌（含本地语言脚本、域名变体、别名等）
  2. 不能是竞争品牌的查询
  3. 父品牌/所有者组合允许（如 "Google Gmail" 对品牌 "Gmail" 有效）
  4. 不能是品牌比较查询
  5. 品牌词 + 无关实体 = 无效（如 "AAMI Park"）
  6. 通用词品牌需明确引用品牌含义
- 验证失败（API 错误/解析失败）时默认标记为有效（保守策略）
- 输出：过滤掉 `AI_Valid = False` 的行，保留最终有效查询

---

## 并发模型

```
process_themes_parallel()
├── ThreadPoolExecutor(max_workers=3)   ← 3 个 theme 并行
│    └── process_theme()
│         └── validate_queries()
│              └── ThreadPoolExecutor(max_workers=10)  ← 10 个验证批次并行
│
│  峰值并发 API 调用：3 × 10 = 30 次 Azure OpenAI 调用
```

---

## 数据流

```
用户输入 themes[]
    │
    ▼
[AI Expand] → expanded_keywords[]         (≤15个，含多语言)
    │
    ▼
[Match]     → results_df                  (含 SRPV, AdClick, revenue, Score, Relevance)
    │
    ▼
[AI Validate] → results_df (含 AI_Valid, AI_Reason 列)
    │
    ▼
过滤 AI_Valid=False → final_df
    │
    ▼
输出 CSV (StreamingResponse / 内嵌在 JSON 响应中)
```

---

## API 端点

| 端点 | 方法 | 描述 |
|------|------|------|
| `GET /` | GET | 前端页面 (`static/index.html`) |
| `GET /api/markets` | GET | 返回可用市场列表及语言配置 |
| `POST /api/expand_stream` | POST | 流式处理（NDJSON），逐 theme 返回结果 |
| `POST /api/expand` | POST | 批量处理，所有 theme 完成后一次性返回 |
| `GET /api/jobs` | GET | 历史任务列表（最近 50 条） |
| `GET /api/jobs/{job_id}` | GET | 指定任务详情 + theme_tasks |
| `GET /api/jobs/{job_id}/themes/{theme_id}` | GET | 指定 theme 详情 + 验证批次 |

---

## 支持的市场

| 市场 | 语言 | Embedding 模型 |
|------|------|----------------|
| Australia | English | all-MiniLM-L6-v2 |
| Philippines | English, Filipino | all-MiniLM-L6-v2 |
| Japan | Japanese, English | paraphrase-multilingual-MiniLM-L12-v2 |
| China | Chinese, English | paraphrase-multilingual-MiniLM-L12-v2 |
| India | English, Hindi | paraphrase-multilingual-MiniLM-L12-v2 |
| Singapore | English, Chinese, Malay | paraphrase-multilingual-MiniLM-L12-v2 |
| Malaysia | Malay, English, Chinese | paraphrase-multilingual-MiniLM-L12-v2 |
| Thailand | Thai, English | paraphrase-multilingual-MiniLM-L12-v2 |
| Indonesia | Indonesian, English | paraphrase-multilingual-MiniLM-L12-v2 |
| Vietnam | Vietnamese, English | paraphrase-multilingual-MiniLM-L12-v2 |

---

## 数据持久化

### 市场数据库（只读，启动时初始化）
- `keywords_<market>.db` — SQLite，来源于 CSV 文件（`<locale>-query.csv`）
- `keywords_<market>.index` — FAISS 向量索引

### 任务历史数据库（运行时写入）
- `logs/jobs.db` — SQLite，三张表：

  | 表 | 说明 |
  |----|------|
  | `jobs` | 任务级记录（market, themes, status, 时间戳）|
  | `theme_tasks` | 每个 theme 的处理过程及结果 |
  | `validation_batches` | 每个验证批次的输入/输出详情 |

### 日志
- `logs/expansion.log` — 按日轮转，保留 30 天；文件记录 DEBUG+，控制台输出 INFO+

---

## 文件结构

```
BZA_keywords_expand/
├── app.py          — FastAPI 服务入口，定义 API 端点
├── main.py         — 核心流程编排（process_theme, process_themes_parallel）
├── ai_expander.py  — Azure OpenAI 调用（扩展 + 验证）
├── matcher.py      — 双路匹配引擎（Hard Match + Vector Match）
├── db_manager.py   — SQLite & FAISS 数据库管理
├── job_store.py    — 任务历史存储（logs/jobs.db）
├── logger.py       — 统一日志配置
├── config.py       — 配置中心（市场、模型、路径、API 参数）
├── static/         — 前端静态文件
└── logs/           — 运行时日志和任务数据库
```

---

## 启动方式

```bash
# Web 服务（推荐）
python app.py
# 访问 http://localhost:7888

# CLI 模式（单 theme）
python main.py --theme "Nike" --market Australia

# CLI 模式（批量文件）
python main.py --file themes.txt --market Japan --output results.csv
```
