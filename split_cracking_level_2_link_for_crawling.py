# ache_seq_crawl.py
# -*- coding: utf-8 -*-
"""
顺序批次爬取：
- 从 seeds.txt 读入所有链接，按 BATCH_SIZE 分组
- 每组创建一个全新的 DeepCrawl（名字带批次号与时间戳）
- 启动后轮询 GET /crawls，直到该 crawlerId 结束，再启动下一组
- 对 startCrawl 出现 5xx 自动重试；仍失败则记录并继续下一组（不阻塞）
- 产生 crawls_log.csv 记录每个批次的启动/结束状态与耗时
"""

import os
import time
import json
import csv
import logging
from typing import List, Optional, Tuple, Dict
import requests

# ============== 配置区（按需修改） ==============
ADMIN_BASE = "http://localhost:8083"           # ACHE Admin API
SEEDS_FILE = "./cracking_forum_urls_level_1_batch_1_to_5_thread_links.txt"      ###################请修改这里################
BATCH_SIZE = 50                                 # 每个 crawl 放多少个链接
CRAWL_NAME_PREFIX = "cracking_forum_urls_level_2_batch_1_to_5" ###################请修改这里################
MAX_START_RETRY = 3                             # startCrawl 失败重试次数
POLL_INTERVAL = 5                               # 轮询 /crawls 的间隔（秒）
MAX_WAIT_SECONDS = 60 * 60                      # 单个批次最多等待（秒），超时就标记 TIMEOUT
HTTP_TIMEOUT = 30                               # 单次 HTTP 超时（秒）
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

        # 结束判定：running 为 False，或 state 落入终止集合
        if running is False or (state and (state.upper() in TERMINAL_STATES)):
            dur = time.time() - start
            logging.info("crawl %s 结束：state=%s，耗时 %.1fs", crawl_name, state, dur)
            return state or "UNKNOWN", dur

        # 如果接口暂时找不到该 crawlerId（None, None），有两种情况：
        # 1) 刚启动未注册；2) 已结束并被清理（某些版本不会长期保留）
        # 这里做个“消失计数”的容错：连续多次拿不到就当作结束。
        # 为简单，这里只做时间超时控制。
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

def main():
    setup_logging()
    seeds = load_seeds(SEEDS_FILE)
    if not seeds:
        logging.error("没有可用链接，退出。")
        return

    batches = chunked(seeds, BATCH_SIZE)
    logging.info("按 %d/批，共 %d 批。", BATCH_SIZE, len(batches))

    csv_rows = []
    header = ["batch_idx", "crawl_name", "num_seeds", "start_ok", "start_code", "start_msg",
              "final_state", "duration_sec"]

    for idx, group in enumerate(batches, start=1):
        crawl_name = f"{CRAWL_NAME_PREFIX}_{idx}_{int(time.time())}"
        logging.info("启动批次 #%d：crawl=%s，links=%d", idx, crawl_name, len(group))

        # 启动 + 重试
        ok, code, msg = False, None, ""
        for attempt in range(1, MAX_START_RETRY + 1):
            ok, code, msg = start_crawl(crawl_name, group)
            if ok:
                logging.info("startCrawl 成功（尝试 #%d）", attempt)
                break
            logging.warning("startCrawl 失败（尝试 #%d）：%s %s", attempt, code, (msg[:200] if msg else ""))
            time.sleep(3 * attempt)

        if not ok:
            logging.error("批次 #%d 启动失败，跳过等待：%s %s", idx, code, (msg[:200] if msg else ""))
            csv_rows.append([idx, crawl_name, len(group), False, code, msg, "NOT_STARTED", 0.0])
            # 不阻塞后续批次；如果你希望“启动失败就停下”，此处可直接 break
            continue

        # 轮询直到结束
        final_state, duration = wait_until_finished(crawl_name)
        csv_rows.append([idx, crawl_name, len(group), True, code, msg, final_state, round(duration, 1)])

        # 可选：若你希望“只要状态不是 TERMINATED/FINISHED 就停止后续批次”，这里判断一下
        # if final_state.upper() not in ("TERMINATED", "FINISHED", "COMPLETED"):
        #     logging.warning("批次 #%d 的最终状态为 %s，停止后续批次。", idx, final_state)
        #     break

        # 为避免名字时间戳重复，批与批之间稍等 1 秒
        time.sleep(1)

    write_log_csv(csv_rows, header)
    logging.info("全部批次处理完成，日志见：%s / %s", LOG_FILE, LOG_CSV)

if __name__ == "__main__":
    main()
