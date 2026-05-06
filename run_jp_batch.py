"""
批量运行 bza_jp_expand_results.xlsx 中所有 theme，输出结果到 CSV。
"""
import sys, io, json, time, requests, pandas as pd
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

EXCEL_PATH  = "Data/TestData/bza_jp_expand_results.xlsx"
OUTPUT_PATH = "Data/TestData/bza_jp_expand_results_new.csv"
API_URL     = "http://localhost:7888/api/expand_stream"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# 读取所有 theme
df_input = pd.read_excel(EXCEL_PATH)
themes = df_input["CoreBrandKeyword"].unique().tolist()
log(f"共 {len(themes)} 个 theme: {themes}")

# 一次性发送所有 theme（服务端 max_workers=3 并行处理）
payload = {"themes": themes, "market": "Japan", "device_type": "pc"}

log("发送请求，开始流式接收...")
t_start = time.time()

all_rows = []
completed_themes = []

resp = requests.post(API_URL, json=payload, stream=True, timeout=600)

for line in resp.iter_lines():
    if not line:
        continue
    s = line.decode("utf-8")
    if not s.startswith("data: "):
        continue
    try:
        event = json.loads(s[6:])
    except Exception:
        continue

    etype = event.get("type")

    if etype == "theme_complete":
        theme   = event.get("theme", "")
        results = event.get("results", [])
        elapsed = time.time() - t_start
        completed_themes.append(theme)
        log(f"✓ [{len(completed_themes)}/{len(themes)}] {theme} — valid={len(results)}  累计={elapsed:.1f}s")

        for r in results:
            all_rows.append({
                "CoreBrandKeyword":  theme,
                "normalized_query":  r.get("normalized_query", ""),
                "Score":             r.get("Score", ""),
                "Relevance":         r.get("Relevance", ""),
                "matched_keyword":   r.get("matched_keyword", ""),
                "SRPV":              r.get("SRPV", ""),
                "AdClick":           r.get("AdClick", ""),
                "revenue":           r.get("revenue", ""),
                "AI_Valid":          r.get("AI_Valid", ""),
                "AI_Reason":         r.get("AI_Reason", ""),
            })

    elif etype == "complete":
        elapsed = time.time() - t_start
        log(f"全部完成！总耗时={elapsed:.1f}s  总条数={len(all_rows)}")

    elif etype == "error":
        log(f"ERROR: {event.get('message')}")

# 保存结果
if all_rows:
    df_out = pd.DataFrame(all_rows)
    df_out.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    log(f"结果已保存 → {OUTPUT_PATH}  ({len(df_out)} 行)")
else:
    log("无结果，未生成文件。")
