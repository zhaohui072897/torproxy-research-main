# ache_seq_crawl_concurrent.py
# -*- coding: utf-8 -*-
"""
并发顺序批次爬取：
- 从 seeds.txt 读入所有链接，按 BATCH_SIZE 分组
- 同时最多并发 MAX_CONCURRENCY 个 DeepCrawl
- 每次启动新的 crawl 之间相隔 START_STAGGER_SECONDS 秒
- 启动后每个批次各自轮询直到结束（线程内完成）
- startCrawl 5xx 自动重试；仍失败则记录并继续
- 产生 crawls_log.csv 记录每个批次的启动/结束状态与耗时
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
SEEDS_FILE = "./cracking_forum_urls_level_1_batch_1_to_5_thread_links.txt"   # ###################请修改这里################
BATCH_SIZE = 50
CRAWL_NAME_PREFIX = "cracking_forum_urls_level_2_batch_1_to_5"               # ###################请修改这里################

MAX_START_RETRY = 3          # startCrawl 失败重试次数
POLL_INTERVAL = 5            # 轮询 /crawls 的间隔（秒）
MAX_WAIT_SECONDS = 60 * 60   # 单个批次最多等待（秒），超时就标记 TIMEOUT
HTTP_TIMEOUT = 30            # 单次 HTTP 超时（秒）

# 新增的并发控制：
MAX_CONCURRENCY = 4          # 同时运行的 crawl 个数上限
START_STAGGER_SECONDS = 60   # 相邻两个启动之间的时间间隔（秒）

LOG_FILE = "./ache_seq_thread_level_2_crawl.log"
LOG_CSV  = "./thread_level_2_crawls_log.csv"
# ===============================================

HEADERS = {"Content-Type": "application/json"}

TERMINAL_STATES = {
    "TERMINATED", "FINISHED", "STOPPED", "KILLED", "FAILED", "COMPLETED"
}

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
    logging.info("总链接数: %d", len(seeds))
    return seeds

def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def start_crawl(crawl_name: str, seeds: List[str]) -> Tuple[bool, int, str]:
    """POST /crawls/{name}/startCrawl，返回 (ok, status_code, text)"""
    url = f"{ADMIN_BASE}/crawls/{crawl_name}/startCrawl"
    payload = {"crawlType": "DeepCrawl", "seeds": seeds, "model": None}
    try:
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=HTTP_TIMEOUT)
        return r.ok, r.status_code, (r.text or "").strip()
    except Exception as e:
        return False, -1, f"EXCEPTION: {e}"

def list_crawls() -> Dict[str, dict]:
    """GET /crawls，返回 {crawlerId: {...}} 映射"""
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
    """返回 (crawlerState, crawlerRunning)；若找不到返回 (None, None)"""
    crawls = list_crawls()
    c = crawls.get(crawl_name)
    if not c:
        return None, None
    return c.get("crawlerState"), c.get("crawlerRunning")

def wait_until_finished(crawl_name: str) -> Tuple[str, float]:
    """
    轮询直到该 crawl 结束。
    返回 (final_state, duration_seconds)；若超时，state='TIMEOUT'
    """
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

_csv_lock = threading.Lock()      # 保护 csv_rows
_csv_rows: List[list] = []
_header = ["batch_idx", "crawl_name", "num_seeds", "start_ok", "start_code", "start_msg",
           "final_state", "duration_sec"]

def worker_run(sema: threading.Semaphore, idx: int, seeds_group: List[str], crawl_name: str):
    """
    单个批次的完整生命周期：启动 -> 等待 -> 记录结果
    结束时释放并发信号量。
    """
    global _csv_rows
    try:
        logging.info("启动批次 #%d：crawl=%s，links=%d", idx, crawl_name, len(seeds_group))

        # 启动 + 重试
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

        # 轮询直到结束
        final_state, duration = wait_until_finished(crawl_name)
        with _csv_lock:
            _csv_rows.append([idx, crawl_name, len(seeds_group), True, code, msg, final_state, round(duration, 1)])

    except Exception as e:
        logging.exception("[#%d] 执行异常：%s", idx, e)
        with _csv_lock:
            _csv_rows.append([idx, crawl_name, len(seeds_group), False, -1, f"EXCEPTION: {e}", "UNKNOWN", 0.0])
    finally:
        sema.release()  # 释放并发名额

def main():
    setup_logging()
    seeds = load_seeds(SEEDS_FILE)
    if not seeds:
        logging.error("没有可用链接，退出。")
        return

    batches = chunked(seeds, BATCH_SIZE)
    logging.info("按 %d/批，共 %d 批。并发上限=%d，启动间隔=%ds",
                 BATCH_SIZE, len(batches), MAX_CONCURRENCY, START_STAGGER_SECONDS)

    sema = threading.Semaphore(MAX_CONCURRENCY)
    threads: List[threading.Thread] = []

    try:
        for idx, group in enumerate(batches, start=1):
            sema.acquire()  # 若达到并发上限，将在此处阻塞直到有空位

            # 为确保名字唯一（并发启动时 time.time() 可能相同），加上毫秒或纳秒
            crawl_name = f"{CRAWL_NAME_PREFIX}_{idx}_{int(time.time()*1000)}"

            t = threading.Thread(target=worker_run, args=(sema, idx, group, crawl_name), daemon=True)
            t.start()
            threads.append(t)

            # 启动间隔：即便并发没满，也遵循相邻启动之间的最小间隔
            time.sleep(START_STAGGER_SECONDS)

        # 等待全部线程完成
        for t in threads:
            t.join()

    except KeyboardInterrupt:
        logging.warning("收到中断信号（Ctrl+C）。已停止发起新批次，等待已启动的批次自行结束...")
        # 不强行终止已启动的 crawl（避免半途而废），只是不再启动新的

    # 写出 CSV
    with _csv_lock:
        write_log_csv(_csv_rows, _header)

    logging.info("全部批次处理完成，日志见：%s / %s", LOG_FILE, LOG_CSV)

if __name__ == "__main__":
    main()
