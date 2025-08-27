# -*- coding: utf-8 -*-
"""
批量扫描 server-cracking 下的 cracking_forum_urls_level_1_seeds_pending_<N>[_ts] 目录，
读取 data_monitor/downloadrequests.csv，分类输出：
- per-batch：good(200)/bad(!=200)/missing(种子里但CSV没出现)/need_attention
- global：all_good_200_urls.txt / all_bad_not200_urls.txt / all_missing_in_csv_from_seeds.txt / all_need_attention_urls.txt
- 新增（基于 output 目录里的总 seeds 文件）：
    all_seeds_succeeded_200.txt          # 总种子 ∩ 所有批次 200
    all_seeds_need_processing.txt        # 总种子 - 所有批次 200   ← 你要的“剩下需要处理”的链接
"""

import os
import re
import csv
import sys
from typing import List, Dict, Tuple, Set, Optional

# =============== 可调参数 ===============
# /output 与 /server-cracking 同级；脚本放在 /output 下运行
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR  = os.path.dirname(SCRIPT_DIR)
SEARCH_ROOT = os.path.join(PARENT_DIR, "server-cracking")  # 扫描根目录

# 目录前缀与范围（注意这里你用的是 seeds_pending 前缀；如需 batch_ 前缀请改下面这一行）
BATCH_PREFIX = "cracking_forum_urls_level_1_seeds_pending_"
BATCH_MIN    = 1
BATCH_MAX    = 58   # 你说的 1-58

# CSV 相对路径；如不存在则在 data_monitor/ 下递归（最多 3 层）搜索同名
CSV_REL_PATH = os.path.join("data_monitor", "downloadrequests.csv")
MAX_DM_DEPTH = 3

# 种子文件优先顺序（仅根目录，不递归）；缺省时再找 seeds*.txt
SEED_PRIORITY_NAMES = ["seeds.txt", "seeds_scope.txt"]
SEED_GLOB_PREFIX    = "seeds"  # 仅根目录，seeds*.txt

# 全量总种子（在 output 下；按需改成你的文件名）
SEEDS_ALL_FILE = os.path.join(SCRIPT_DIR, "cracking_forum_urls_level_1_seeds_pending.txt")

# 输出根目录
OUT_DIR = "monitor_split"
# ======================================

def dprint(*args):
    print(*args, flush=True)

BATCH_DIR_REGEX = re.compile(
    rf'^{re.escape(BATCH_PREFIX)}(?P<idx>\d+)(?:_(?P<ts>\d+))?$',
    re.IGNORECASE
)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def sniff_delimiter_and_header(sample_path: str) -> Tuple[str, bool]:
    """嗅探分隔符 & 是否有表头；失败则默认制表符 + 无表头（适配常见格式）"""
    try:
        with open(sample_path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(8192)
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        has_header = csv.Sniffer().has_header(sample)
        return dialect.delimiter, has_header
    except Exception:
        return "\t", False

def read_csv_rows(csv_path: str) -> Tuple[Optional[List[str]], List[List[str]], str, bool]:
    """
    读取 CSV：
    返回 (header 或 None, data_rows, delimiter, has_header)
    - 无表头时，header=None，data_rows 包含所有行
    """
    delimiter, has_header = sniff_delimiter_and_header(csv_path)
    try:
        f = open(csv_path, "r", encoding="utf-8-sig", newline="")
    except Exception:
        f = open(csv_path, "r", encoding="utf-8", errors="replace", newline="")
    with f:
        rdr = csv.reader(f, delimiter=delimiter)
        rows = list(rdr)
        if not rows:
            return None, [], delimiter, has_header
        if has_header:
            header = rows[0]
            data = rows[1:] if len(rows) > 1 else []
            return header, data, delimiter, True
        else:
            return None, rows, delimiter, False

def locate_col(header: List[str], candidates: List[str], contains: Optional[str]=None, default_idx: Optional[int]=None) -> int:
    if not header:
        return -1
    lower = [ (c or "").strip().lower() for c in header ]
    cand_set = { c.lower() for c in candidates }
    for i, lc in enumerate(lower):
        if lc in cand_set:
            return i
    if contains:
        for i, lc in enumerate(lower):
            if contains in lc:
                return i
    return default_idx if default_idx is not None else -1

def find_csv_cols(header: List[str]) -> Tuple[int,int]:
    url_idx = locate_col(header, ["url","request","requesturl","link","target","address"],
                         contains="url", default_idx=len(header)-1 if header else -1)
    st_idx  = locate_col(header, ["status","statuscode","http_status","http_code","code","responsecode"],
                         contains="status", default_idx=1 if len(header) > 1 else -1)
    return url_idx, st_idx

def parse_status_code(raw: str) -> Optional[int]:
    if raw is None:
        return None
    m = re.search(r"\b(\d{3})\b", str(raw))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def find_batch_dirs(root: str) -> Dict[int, List[str]]:
    dprint(f"[PATH] SCRIPT_DIR  = {SCRIPT_DIR}")
    dprint(f"[PATH] PARENT_DIR  = {PARENT_DIR}")
    dprint(f"[PATH] SEARCH_ROOT = {SEARCH_ROOT} (exists: {os.path.isdir(SEARCH_ROOT)})")

    found: Dict[int, List[str]] = {}
    if not os.path.isdir(root):
        dprint(f"[ERROR] 搜索根目录不存在: {root}")
        return found

    entries = [n for n in os.listdir(root) if os.path.isdir(os.path.join(root, n))]
    dprint(f"[INFO] 在 {root} 下发现目录 {len(entries)} 个；前10个：{entries[:10]}")

    for name in entries:
        m = BATCH_DIR_REGEX.match(name)
        if not m:
            continue
        idx = int(m.group("idx"))
        if idx < BATCH_MIN or idx > BATCH_MAX:
            continue
        full = os.path.join(root, name)
        found.setdefault(idx, []).append(full)

    if not found:
        candidates = [n for n in entries if n.lower().startswith("cracking_forum_urls_level_1_")]
        dprint(f"[WARN] 未匹配到批次目录，当前前缀为 `{BATCH_PREFIX}`。疑似目录样例：{candidates[:10]}")
        return {}

    for i in list(found.keys()):
        found[i].sort(key=lambda p: os.path.getmtime(p), reverse=True)

    dprint(f"[INFO] 命中批次索引：{sorted(found.keys())}")
    for i, dirs in found.items():
        dprint(f"  - batch index {i}: latest={os.path.basename(dirs[0])} (total versions={len(dirs)})")
    return dict(sorted(found.items(), key=lambda kv: kv[0]))

def find_csv_in_batch(batch_dir: str) -> Optional[str]:
    fixed = os.path.join(batch_dir, CSV_REL_PATH)
    if os.path.isfile(fixed):
        return fixed
    dm_root = os.path.join(batch_dir, "data_monitor")
    if not os.path.isdir(dm_root):
        return None
    base_depth = dm_root.rstrip(os.sep).count(os.sep)
    for dp, _dirs, files in os.walk(dm_root):
        cur_depth = dp.rstrip(os.sep).count(os.sep) - base_depth
        if cur_depth > MAX_DM_DEPTH:
            continue
        for fn in files:
            if fn == os.path.basename(CSV_REL_PATH):
                return os.path.join(dp, fn)
    return None

def find_seed_files(batch_dir: str) -> List[str]:
    out: List[str] = []
    for name in SEED_PRIORITY_NAMES:
        p = os.path.join(batch_dir, name)
        if os.path.isfile(p):
            out.append(p)
    for name in os.listdir(batch_dir):
        if name in SEED_PRIORITY_NAMES:
            continue
        if not name.lower().endswith(".txt"):
            continue
        if not name.lower().startswith(SEED_GLOB_PREFIX):
            continue
        p = os.path.join(batch_dir, name)
        if os.path.isfile(p):
            out.append(p)
    uniq, seen = [], set()
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq

def read_seeds(paths: List[str]) -> Tuple[List[str], Set[str]]:
    ordered, seen = [], set()
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s: 
                        continue
                    if s in seen:
                        continue
                    seen.add(s)
                    ordered.append(s)
        except Exception as e:
            dprint(f"[WARN] 读取种子失败：{p} -> {e}")
    return ordered, seen

def read_all_seeds(path: str) -> Tuple[List[str], Set[str]]:
    """读取 output 下的总 seeds 文件：去空、去重、保序"""
    ordered, seen = [], set()
    if not os.path.isfile(path):
        dprint(f"[WARN] 全量种子文件不存在：{path}")
        return ordered, seen
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s in seen:
                continue
            seen.add(s)
            ordered.append(s)
    return ordered, seen

def write_lines(path: str, lines: List[str]):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as w:
        for s in lines:
            w.write(s + "\n")

def write_csv(path: str, header: Optional[List[str]], rows: List[List[str]], delimiter: str, include_header: bool):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f, delimiter=delimiter)
        if include_header and header:
            wr.writerow(header)
        for r in rows:
            wr.writerow(r)

def main():
    dprint(f"[INFO] Python: {sys.version}")
    dprint(f"[INFO] 工作目录: {os.getcwd()}")
    dprint(f"[INFO] SEARCH_ROOT = {SEARCH_ROOT}")
    dprint(f"[INFO] SEEDS_ALL_FILE = {SEEDS_ALL_FILE} (exists: {os.path.isfile(SEEDS_ALL_FILE)})")

    idx2dirs = find_batch_dirs(SEARCH_ROOT)
    if not idx2dirs:
        dprint("[WARN] 未找到任何批次目录（检查范围/前缀/路径）。")
        return

    # 全局汇总（只汇总 URL，便于后续处理）
    g_good_urls, g_bad_urls, g_missing_urls, g_need_urls = [], [], [], []
    g_seen_good, g_seen_bad, g_seen_missing, g_seen_need = set(), set(), set(), set()

    # 遍历每个“实际目录名”（含时间戳），逐个输出，目录名即输出子目录名
    for idx, dirs in idx2dirs.items():
        for batch_dir in dirs:
            batch_name = os.path.basename(batch_dir)  # 原目录名
            out_dir = os.path.join(OUT_DIR, batch_name)
            dprint(f"\n[PROC] >>> {batch_name}")

            # 读取 CSV
            csv_path = find_csv_in_batch(batch_dir)
            header, data_rows, delimiter, has_header = (None, [], ",", False)
            if csv_path:
                header, data_rows, delimiter, has_header = read_csv_rows(csv_path)
                dprint(f"  [CSV] path={csv_path}")
                dprint(f"  [CSV] delimiter={repr(delimiter)}  has_header={has_header}  rows={len(data_rows)}")
            else:
                dprint(f"  [CSV] 未找到 {CSV_REL_PATH}，跳过该目录的 CSV 解析")

            # 定位列 & 无表头兜底
            if header:
                url_idx, st_idx = find_csv_cols(header)
                dprint(f"  [CSV] url_idx={url_idx}  status_idx={st_idx}")
            else:
                # 无表头：兜底规则→ status=第2列，url=最后一列
                url_idx, st_idx = -1, 1
                dprint("  [CSV] 无表头 → 兜底规则：status=第2列，url=最后一列")

            # 从 CSV 取 URL 集合 + 拆分 200 / 非200
            csv_urls_seen: Set[str] = set()
            good_rows, bad_rows = [], []
            good_urls, bad_urls = [], []

            if data_rows:
                for row in data_rows:
                    if header and len(row) < len(header):
                        row = row + [""] * (len(header) - len(row))
                    # URL
                    url = row[-1].strip() if not header else (row[url_idx].strip() if 0 <= url_idx < len(row) else "")
                    if url:
                        csv_urls_seen.add(url)
                    # 状态码
                    raw_status = row[st_idx] if (0 <= st_idx < len(row)) else ""
                    code = parse_status_code(raw_status)
                    if code == 200:
                        good_rows.append(row)
                        if url and url not in good_urls:
                            good_urls.append(url)
                    elif code is not None and code != 200:
                        bad_rows.append(row)
                        if url and url not in bad_urls:
                            bad_urls.append(url)
                dprint(f"  [CSV] parsed: csv_urls={len(csv_urls_seen)}  good=200:{len(good_urls)}  bad!=200:{len(bad_urls)}")
            else:
                dprint("  [CSV] 没有可解析的数据行")

            # 找 seeds（当前批次根目录）
            seed_files = find_seed_files(batch_dir)
            dprint(f"  [SEEDS] files={len(seed_files)} -> {seed_files[:3]}")
            seeds_order, seeds_set = read_seeds(seed_files) if seed_files else ([], set())
            dprint(f"  [SEEDS] total={len(seeds_set)}")

            # 计算 “种子里但 CSV 没出现”的链接
            missing_urls = [u for u in seeds_order if u not in csv_urls_seen]
            dprint(f"  [DIFF] missing_in_csv_from_seeds={len(missing_urls)}")

            # 需要关注 = 非200 ∪ 缺失（去重保序）
            need_urls = []
            seen_tmp = set()
            for u in bad_urls + missing_urls:
                if u not in seen_tmp:
                    seen_tmp.add(u)
                    need_urls.append(u)
            dprint(f"  [DIFF] need_attention={len(need_urls)}")

            # ==== 按原目录名输出 ====
            if header is not None:
                write_csv(os.path.join(out_dir, "good_200.csv"), header, good_rows, delimiter, include_header=True)
                write_csv(os.path.join(out_dir, "bad_not200.csv"), header, bad_rows, delimiter, include_header=True)
            else:
                write_csv(os.path.join(out_dir, "good_200.csv"), None, good_rows, delimiter, include_header=False)
                write_csv(os.path.join(out_dir, "bad_not200.csv"), None, bad_rows, delimiter, include_header=False)

            write_lines(os.path.join(out_dir, "good_200_urls.txt"), good_urls)
            write_lines(os.path.join(out_dir, "bad_not200_urls.txt"), bad_urls)
            write_lines(os.path.join(out_dir, "missing_in_csv_from_seeds.txt"), missing_urls)
            write_lines(os.path.join(out_dir, "need_attention.txt"), need_urls)
            dprint(f"  [OUT] {out_dir}")

            # ==== 累计全局汇总（只汇总 URL） ====
            for u in good_urls:
                if u not in g_seen_good:
                    g_seen_good.add(u); g_good_urls.append(u)
            for u in bad_urls:
                if u not in g_seen_bad:
                    g_seen_bad.add(u); g_bad_urls.append(u)
            for u in missing_urls:
                if u not in g_seen_missing:
                    g_seen_missing.add(u); g_missing_urls.append(u)
            for u in need_urls:
                if u not in g_seen_need:
                    g_seen_need.add(u); g_need_urls.append(u)

            dprint(f"[{batch_name}] SUMMARY | CSV={len(csv_urls_seen)}  good=200:{len(good_urls)}  "
                   f"bad!=200:{len(bad_urls)}  seeds={len(seeds_set)}  missing:{len(missing_urls)}  need:{len(need_urls)}")

    # ==== 全局汇总输出 ====
    ensure_dir(OUT_DIR)
    write_lines(os.path.join(OUT_DIR, "all_good_200_urls.txt"), g_good_urls)
    write_lines(os.path.join(OUT_DIR, "all_bad_not200_urls.txt"), g_bad_urls)
    write_lines(os.path.join(OUT_DIR, "all_missing_in_csv_from_seeds.txt"), g_missing_urls)
    write_lines(os.path.join(OUT_DIR, "all_need_attention_urls.txt"), g_need_urls)

    # ==== 新增：基于“总 seeds 文件”的差集 ====
    seeds_all_order, seeds_all_set = read_all_seeds(SEEDS_ALL_FILE)
    g_good_set = set(g_good_urls)

    seeds_succeeded = [u for u in seeds_all_order if u in g_good_set]
    seeds_need_proc = [u for u in seeds_all_order if u not in g_good_set]  # ← 要的结果

    write_lines(os.path.join(OUT_DIR, "all_seeds_succeeded_200.txt"), seeds_succeeded)
    write_lines(os.path.join(OUT_DIR, "all_seeds_need_processing.txt"), seeds_need_proc)

    dprint(f"\n[GLOBAL] total_seeds={len(seeds_all_order)}  succeeded_200={len(seeds_succeeded)}  "
           f"need_processing={len(seeds_need_proc)}")

    # 汇总报告
    with open(os.path.join(OUT_DIR, "monitor_summary.txt"), "w", encoding="utf-8") as w:
        w.write("=== DownloadRequests Monitor Summary ===\n")
        w.write(f"SEARCH_ROOT      : {SEARCH_ROOT}\n")
        w.write(f"Prefix & Range   : {BATCH_PREFIX}[{BATCH_MIN}..{BATCH_MAX}]\n")
        w.write("\n")
        w.write(f"ALL good=200     : {len(g_good_urls)} -> all_good_200_urls.txt\n")
        w.write(f"ALL bad!=200     : {len(g_bad_urls)} -> all_bad_not200_urls.txt\n")
        w.write(f"ALL missing_csv  : {len(g_missing_urls)} -> all_missing_in_csv_from_seeds.txt\n")
        w.write(f"ALL need_attention: {len(g_need_urls)} -> all_need_attention_urls.txt\n")
        w.write("\n")
        w.write(f"SEEDS_ALL_FILE   : {SEEDS_ALL_FILE}\n")
        w.write(f"Seeds total      : {len(seeds_all_order)}\n")
        w.write(f"Seeds ∩ good200  : {len(seeds_succeeded)} -> all_seeds_succeeded_200.txt\n")
        w.write(f"Seeds minus 200  : {len(seeds_need_proc)} -> all_seeds_need_processing.txt\n")

    dprint(f"[DONE] 输出目录：{os.path.abspath(OUT_DIR)}")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
