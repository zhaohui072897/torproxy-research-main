# -*- coding: utf-8 -*-
"""
根据兄弟目录 cracking_forum_urls_level_1_batch_<N>[_timestamp] 下的 seeds_scope.txt
来判断哪些链接“已跑”，其余视为“未跑”。输出 done/pending 清单与汇总 TXT。
"""

import os
import re
from typing import List, Dict, Tuple, Set

# ========= 内置配置（按需修改） =========
SEEDS_FILE    = "cracking_forum_urls_level_1.txt"   # 全量待跑 seeds（每行一个 URL）
OUT_DIR       = "seeds_split"                       # 输出目录
BATCH_SIZE    = 100                                  # 未跑的链接按此大小重新分批导出

# CHANGED: 只保留目录名前缀，不要带路径
BATCH_PREFIX  = "cracking_forum_urls_level_1_batch_"  # 兄弟目录前缀

BATCH_MIN     = 1                                    # 扫描 batch_1 ...
BATCH_MAX     = 38                                   # ... 到 batch_37（含）
SEEDS_SCOPE_BASENAME = "seeds_scope.txt"             # 在各 batch 目录内寻找的文件名
# =====================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# CHANGED: /output 与 /server-cracking 同级，这里指向同级的 server-cracking
PARENT_DIR  = os.path.dirname(SCRIPT_DIR)                  # e.g. "/"
SEARCH_ROOT = os.path.join(PARENT_DIR, "server-cracking")  # e.g. "/server-cracking"

BATCH_DIR_REGEX = re.compile(
    rf'^{re.escape(BATCH_PREFIX)}(?P<idx>\d+)(?:_(?P<ts>\d+))?$',
    re.IGNORECASE
)

def ensure_dir(d: str):
    os.makedirs(d, exist_ok=True)

def load_seeds(path: str) -> List[str]:
    """读取全量 seeds，去空行、去重但保留首次顺序。"""
    if not os.path.exists(path):
        print(f"[ERROR] 找不到 seeds 文件：{path}")
        return []
    seen, out = set(), []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
    return out

def find_batch_dirs(root: str) -> Dict[int, List[str]]:
    """
    在 root 下查找形如 cracking_forum_urls_level_1_batch_<idx>[_timestamp] 的目录。
    返回 {idx: [dir1, dir2, ...]}，同一 idx 可能存在多个时间戳版本。
    """
    found: Dict[int, List[str]] = {}
    if not os.path.isdir(root):
        print(f"[WARN] 搜索根目录不存在：{root}")
        return found

    for name in os.listdir(root):
        full = os.path.join(root, name)
        if not os.path.isdir(full):
            continue
        m = BATCH_DIR_REGEX.match(name)  # 注意：这里匹配的是目录名，不含路径
        if not m:
            continue
        idx = int(m.group("idx"))
        if idx < BATCH_MIN or idx > BATCH_MAX:
            continue
        found.setdefault(idx, []).append(full)

    # 按 idx 排序；每个 idx 目录列表按 mtime 降序（新的在前），但后续会合并所有 seeds_scope
    for i in list(found.keys()):
        found[i].sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return dict(sorted(found.items(), key=lambda kv: kv[0]))

def collect_done_seeds_from_batch_dir(batch_dir: str) -> Tuple[Set[str], List[str]]:
    """
    在 batch_dir 下搜索 seeds_scope.txt（可能不只一份），合并其内容为集合。
    也返回找到的 seeds_scope 文件路径列表，便于记录。
    """
    done: Set[str] = set()
    found_files: List[str] = []

    # 方案：优先查目录根；找不到则在该目录下递归搜索文件名精确为 seeds_scope.txt
    candidate = os.path.join(batch_dir, SEEDS_SCOPE_BASENAME)
    if os.path.isfile(candidate):
        found_files.append(candidate)
    else:
        # 避免过深递归造成开销，这里限制最大搜索深度为 3 层
        max_depth = 3
        base_depth = batch_dir.rstrip(os.sep).count(os.sep)
        for dp, _, fns in os.walk(batch_dir):
            cur_depth = dp.rstrip(os.sep).count(os.sep) - base_depth
            if cur_depth > max_depth:
                continue
            for fn in fns:
                if fn == SEEDS_SCOPE_BASENAME:
                    found_files.append(os.path.join(dp, fn))

    for path in found_files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s:
                        done.add(s)
        except Exception as e:
            print(f"[WARN] 读取失败：{path} -> {e}")

    return done, found_files

def write_lines(path: str, lines: List[str]):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as w:
        for s in lines:
            w.write(s + "\n")

def dump_batches(batches: Dict[int, List[str]], outdir: str, prefix="batch_"):
    ensure_dir(outdir)
    for i in sorted(batches.keys()):
        p = os.path.join(outdir, f"{prefix}{i:04d}.txt")
        with open(p, "w", encoding="utf-8") as w:
            w.write("\n".join(batches[i]))

def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def write_summary(path: str, summary: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as w:
        w.write(summary)

def main():
    print(f"[INFO] 脚本目录：{SCRIPT_DIR}")
    print(f"[INFO] 搜索根目录（SEARCH_ROOT）：{SEARCH_ROOT}")
    seeds_all = load_seeds(SEEDS_FILE)
    if not seeds_all:
        print("[ERROR] 全量 seeds 为空，退出。"); return

    # 1) 找 batch_1..batch_37* 目录
    idx2dirs = find_batch_dirs(SEARCH_ROOT)
    if not idx2dirs:
        print(f"[WARN] 在 {SEARCH_ROOT} 未找到 {BATCH_PREFIX}[{BATCH_MIN}..{BATCH_MAX}] 的目录。")

    # 2) 读取每个 batch 的 seeds_scope.txt，合并为已跑集合
    done_set: Set[str] = set()
    per_batch_done: Dict[int, List[str]] = {}
    missing_scope: List[int] = []
    details_lines: List[str] = []

    for idx, dirs in idx2dirs.items():
        batch_done: Set[str] = set()
        batch_found_paths: List[str] = []
        for d in dirs:
            d_done, found_paths = collect_done_seeds_from_batch_dir(d)
            batch_done |= d_done
            batch_found_paths.extend(found_paths)
        if batch_done:
            per_batch_done[idx] = sorted(batch_done)
            done_set |= batch_done
            details_lines.append(f"batch_{idx:04d}: {len(batch_done)} links, files={len(batch_found_paths)}")
        else:
            missing_scope.append(idx)
            details_lines.append(f"batch_{idx:04d}: 0 links (no {SEEDS_SCOPE_BASENAME} found)")

    # 3) 按原顺序划分 done / pending
    seeds_done: List[str] = []
    seeds_pending: List[str] = []
    for s in seeds_all:
        (seeds_done if s in done_set else seeds_pending).append(s)

    # 4) 输出
    ensure_dir(OUT_DIR)
    path_done    = os.path.join(OUT_DIR, "seeds_done.txt")
    path_pending = os.path.join(OUT_DIR, "seeds_pending.txt")
    write_lines(path_done, seeds_done)
    write_lines(path_pending, seeds_pending)

    # 已跑批次：按 seeds_scope 写出
    dump_batches(per_batch_done, os.path.join(OUT_DIR, "batches_done"))

    # 未跑批次：按 BATCH_SIZE 重新分组
    pending_batches_list = chunked(seeds_pending, BATCH_SIZE)
    pending_idx_map = {i+1: grp for i, grp in enumerate(pending_batches_list)}
    dump_batches(pending_idx_map, os.path.join(OUT_DIR, "batches_pending"))

    # 5) 汇总 TXT
    summary_lines = []
    summary_lines.append("=== Seeds Split Summary (by seeds_scope.txt) ===")
    summary_lines.append(f"Search root          : {SEARCH_ROOT}")
    summary_lines.append(f"Batch prefix         : {BATCH_PREFIX}")
    summary_lines.append(f"Batch range          : {BATCH_MIN}..{BATCH_MAX}")
    summary_lines.append(f"Total links (all)    : {len(seeds_all)}")
    summary_lines.append(f"Done links (by scope): {len(seeds_done)}")
    summary_lines.append(f"Pending links        : {len(seeds_pending)}")
    summary_lines.append("")
    summary_lines.append(f"Found batch indexes  : {', '.join(map(str, sorted(idx2dirs.keys()))) or '(none)'}")
    if missing_scope:
        summary_lines.append(f"WARNING no seeds_scope: {', '.join(map(str, missing_scope))}")
    summary_lines.append("")
    summary_lines.append("Per-batch details:")
    summary_lines.extend(details_lines)
    summary_lines.append("")
    summary_lines.append("Outputs:")
    summary_lines.append(f"- {os.path.relpath(path_done, OUT_DIR)}")
    summary_lines.append(f"- {os.path.relpath(path_pending, OUT_DIR)}")
    summary_lines.append(f"- batches_done/batch_XXXX.txt")
    summary_lines.append(f"- batches_pending/batch_XXXX.txt")
    write_summary(os.path.join(OUT_DIR, "split_summary.txt"), "\n".join(summary_lines))

    # 控制台提示
    print(f"[DONE] 已跑链接: {len(seeds_done)}  → {path_done}")
    print(f"[DONE] 未跑链接: {len(seeds_pending)} → {path_pending}")
    print(f"[INFO] 批次清单导出：{os.path.join(OUT_DIR, 'batches_done')} / {os.path.join(OUT_DIR, 'batches_pending')}")
    print(f"[INFO] 汇总已打印到：{os.path.join(OUT_DIR, 'split_summary.txt')}")

if __name__ == "__main__":
    main()
