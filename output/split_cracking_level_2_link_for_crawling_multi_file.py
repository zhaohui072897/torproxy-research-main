# ache_seq_crawl_concurrent.py
# -*- coding: utf-8 -*-
"""
支持多个 seeds 文件：
- 每个 seeds 文件独立生成批次
- crawl_name 前缀自动取 seeds 文件名（去掉路径和扩展名）
"""

import os
import time
import json
import csv
import logging
import threading
from typing import List, Optional, Tuple, Dict
import requests

# ============== 配置区（按需修改） ==============
ADMIN_BASE = "http://localhost:8083"  # ACHE Admin API

SEEDS_FILES = [
    "./all_seeds_need_processing_level_2_batch_6.txt",
    "./all_seeds_need_processing_level_2_batch_13.txt",
    "./all_seeds_need_processing_level_2_batch_21_to_25.txt",
]

BATCH_SIZE = 100
MAX_START_RETRY = 3
POLL_INTERVAL = 5
MAX_WAIT_SECONDS = 60 * 60
HTTP_TIMEOUT = 30

MAX_CONCURRENCY = 5
START_STAGGER_SECONDS = 60

LOG_FILE = "./ache_seq_thread_failed.log"
LOG_CSV  = "./thread_failed.csv"
# ===============================================

HEADERS = {"Content-Type": "application/json"}
TERMINAL_STATES = {
    "TERMINATED", "FINISHED", "STOPPED", "KILLED", "FAILED", "COMPLETED"
}

# ---------------- 基础函数 ----------------

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")]
    )

def load_seeds(path: str) -> List[str]:
    seeds, seen = [], set()
    if not os.path.exists(path):
        logging.error("找不到种子文件: %s", path)
        return seeds
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and s not in seen:
                seen.add(s)
                seeds.append(s)
    logging.info("[%s] 总链接数: %d", os.path.basename(path), len(seeds))
    return seeds

def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def start_crawl(crawl_name: str, seeds: List[str]) -> Tuple[bool, int, str]:
    url = f"{ADMIN_BASE}/crawls/{crawl_name}/startCrawl"
    payload = {"crawlType": "DeepCrawl", "seeds": seeds, "model": None}
    try:
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=HTTP_TIMEOUT)
        return r.ok, r.status_code, (r.text or "").strip()
    except Exception as e:
        return False, -1, f"EXCEPTION: {e}"

def list_crawls() -> Dict[str, dict]:
    url = f"{ADMIN_BASE}/crawls"
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json() if r.text else {}
        out = {}
        for c in (data.get("crawlers") or []):
            cid = c.get("crawlerId")
            if cid:
                out[cid] = c
        return out
    except Exception as e:
        logging.warning("拉取 /crawls 失败: %s", e)
        return {}

def get_crawl_state(crawl_name: str) -> Tuple[Optional[str], Optional[bool]]:
    crawls = list_crawls()
    c = crawls.get(crawl_name)
    if not c:
        return None, None
    return c.get("crawlerState"), c.get("crawlerRunning")

def wait_until_finished(crawl_name: str) -> Tuple[str, float]:
    start = time.time()
    last_state = None
    while True:
        state, running = get_crawl_state(crawl_name)
        if state is not None:
            last_state = state
        if running is False or (state and (state.upper() in TERMINAL_STATES)):
            dur = time.time() - start
            logging.info("crawl %s 结束：state=%s，耗时 %.1fs", crawl_name, state, dur)
            return state or "UNKNOWN", dur
        if (time.time() - start) > MAX_WAIT_SECONDS:
            logging.warning("等待超时：crawl %s 超过 %ds 未结束", crawl_name, MAX_WAIT_SECONDS)
            return "TIMEOUT", time.time() - start
        time.sleep(POLL_INTERVAL)

def write_log_csv(rows: List[list], header: List[str]):
    newfile = not os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", encoding="utf-8-sig", newline="") as cf:
        w = csv.writer(cf)
        if newfile:
            w.writerow(header)
        for r in rows:
            w.writerow(r)

# ---------------- 并发部分 ----------------
_csv_lock = threading.Lock()
_csv_rows: List[list] = []
_header = ["batch_idx", "crawl_name", "num_seeds", "start_ok", "start_code", "start_msg",
           "final_state", "duration_sec"]

def worker_run(sema: threading.Semaphore, idx: int, seeds_group: List[str], crawl_name: str):
    global _csv_rows
    try:
        logging.info("启动批次 #%d：crawl=%s，links=%d", idx, crawl_name, len(seeds_group))
        ok, code, msg = False, None, ""
        for attempt in range(1, MAX_START_RETRY + 1):
            ok, code, msg = start_crawl(crawl_name, seeds_group)
            if ok:
                logging.info("[#%d] startCrawl 成功（尝试 #%d）", idx, attempt)
                break
            logging.warning("[#%d] startCrawl 失败（尝试 #%d）：%s %s", idx, attempt, code, (msg[:200] if msg else ""))
            time.sleep(3 * attempt)
        if not ok:
            logging.error("[#%d] 启动失败，跳过等待：%s %s", idx, code, (msg[:200] if msg else ""))
            with _csv_lock:
                _csv_rows.append([idx, crawl_name, len(seeds_group), False, code, msg, "NOT_STARTED", 0.0])
            return
        final_state, duration = wait_until_finished(crawl_name)
        with _csv_lock:
            _csv_rows.append([idx, crawl_name, len(seeds_group), True, code, msg, final_state, round(duration, 1)])
    except Exception as e:
        logging.exception("[#%d] 执行异常：%s", idx, e)
        with _csv_lock:
            _csv_rows.append([idx, crawl_name, len(seeds_group), False, -1, f"EXCEPTION: {e}", "UNKNOWN", 0.0])
    finally:
        sema.release()

def main():
    setup_logging()
    sema = threading.Semaphore(MAX_CONCURRENCY)
    threads: List[threading.Thread] = []

    try:
        for seeds_file in SEEDS_FILES:
            seeds = load_seeds(seeds_file)
            if not seeds:
                continue
            batches = chunked(seeds, BATCH_SIZE)
            prefix = os.path.splitext(os.path.basename(seeds_file))[0]  # 文件名作前缀
            logging.info("[%s] 按 %d/批，共 %d 批", prefix, BATCH_SIZE, len(batches))

            for idx, group in enumerate(batches, start=1):
                sema.acquire()
                crawl_name = f"{prefix}_{idx}_{int(time.time()*1000)}"
                t = threading.Thread(target=worker_run, args=(sema, idx, group, crawl_name), daemon=True)
                t.start()
                threads.append(t)
                time.sleep(START_STAGGER_SECONDS)

        for t in threads:
            t.join()

    except KeyboardInterrupt:
        logging.warning("收到中断信号，等待已启动批次结束...")

    with _csv_lock:
        write_log_csv(_csv_rows, _header)

    logging.info("全部完成，日志见：%s / %s", LOG_FILE, LOG_CSV)

if __name__ == "__main__":
    main()
