# -*- coding: utf-8 -*-
"""
extract_xenforo_thread_links_fixed.py

功能：
- 从多个“批次目录”中提取 XenForo 线程列表链接。批次目录名形如：
    cracking_forum_urls_level_1_batch_1_xxxxx
    cracking_forum_urls_level_1_batch_2_yyyyy
  即“固定前缀 + batch_<编号> + 随机后缀”。脚本会按指定范围（含端点）
  扫描所有以这些前缀开头的目录。

- 仅抓取 div.structItem-title 下的 a[href]，并归一化为 /threads/<slug.id>/ 形式：
    去掉 /unread、/page-N、以及 ?query 等附加部分
  然后补全为绝对链接（加 BASE_ORIGIN）。

- 排序与去重：按 (batch编号, page页码, 文件顺序, DOM顺序) 排序，去重保留首见。

- 输出：
  1) 控制台：打印所有去重后的链接与统计信息
  2) 文件：TXT（每行一个链接）与 CSV（列：batch, dir, json, page, link）

用法：
  直接运行：
    python extract_xenforo_thread_links_fixed.py
"""

import os
import re
import json
import csv
from urllib.parse import urlsplit, parse_qs
from bs4 import BeautifulSoup

# ========【全部常量：按需修改】========
PARENT_DIR    = "./cracking-to-json"                          # 批次目录所在上级目录
BATCH_PREFIX  = "cracking_forum_urls_level_2_batch_"          # 批次目录“固定前缀”部分
BATCH_START   = 8                                             # 起始批次（含）
BATCH_END     = 9                                             # 结束批次（含）
RECURSIVE     = True                                          # 是否递归遍历子目录
BASE_ORIGIN   = "https://cracking.org"                        # 域名，用于补全 /threads/...
OUTPUT_DIR    = "./level_1_url_from_json"                     # 输出根目录
SITE_KEY      = "cracking_forum"                              # 二级目录名
ENCODING      = "utf-8"
CSV_ENCODING  = "utf-8-sig"
# =====================================

# —— 自动计算输出文件名（包含批次范围）
OUT_TAG = f"{BATCH_PREFIX.rstrip('_')}_{BATCH_START}_to_{BATCH_END}"
SITE_OUT_DIR = os.path.join(OUTPUT_DIR, SITE_KEY)
OUT_TXT = os.path.join(SITE_OUT_DIR, f"{OUT_TAG}_thread_links.txt")
OUT_CSV = os.path.join(SITE_OUT_DIR, f"{OUT_TAG}_thread_links.csv")


def iter_json_files(root, recursive=True):
    if recursive:
        for dp, _, fns in os.walk(root):
            for fn in fns:
                if fn.lower().endswith(".json"):
                    yield os.path.join(dp, fn)
    else:
        for fn in os.listdir(root):
            if fn.lower().endswith(".json"):
                yield os.path.join(root, fn)


def natural_key_for_path(p: str):
    """对路径的 basename 做自然排序键，例如 2.json < 10.json。"""
    name = os.path.basename(p)
    parts = re.split(r'(\d+)', name)
    return [int(s) if s.isdigit() else s.lower() for s in parts]


def extract_page_from_url(url: str) -> int:
    """从 URL 推断页码：优先 ?page=；其次 /page-N 结构；未命中返回 1。"""
    if not url:
        return 1
    u = urlsplit(url)
    q = parse_qs(u.query)
    if "page" in q and q["page"]:
        m = re.match(r"^\d+", q["page"][0])
        if m:
            try:
                return int(m.group(0))
            except ValueError:
                pass
    # 兼容 XenForo 的 /page-N 形式
    m2 = re.search(r"/page-(\d+)(/|$)", u.path or "")
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            pass
    return 1


def pick_page_url(rec: dict) -> str:
    # 以 url 为准（若无再用 redirected_url）
    return rec.get("url") or rec.get("redirected_url") or ""


def normalize_thread_path(href: str) -> str:
    """
    归一化线程链接路径：
    - 仅处理 /threads/... 开头
    - 截断为 /threads/<slug.id>/ 形式（去掉 /unread、/page-N 等附加路径与 query）
    """
    if not href:
        return ""
    u = urlsplit(href)
    path = u.path or ""
    if not path.startswith("/threads/"):
        return ""

    # /threads/<slug.id>/... → 只保留前两个片段
    parts = [p for p in path.split("/") if p]
    # 期望 parts: ["threads", "hello0.284739", ...]
    if len(parts) < 2:
        return ""

    base = f"/{parts[0]}/{parts[1]}/"  # "/threads/hello0.284739/"
    return base


def to_abs_url(path_or_url: str) -> str:
    """相对路径补全为绝对 URL。"""
    if not path_or_url:
        return ""
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    return BASE_ORIGIN.rstrip("/") + path_or_url


def extract_thread_links_from_content(html: str):
    """
    返回 [full_url,...]，仅保留指向 /threads/<slug.id>/ 的线程链接；
    - 通过 div.structItem-title 下的 <a href> 检索
    - 保持 DOM 原始顺序
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    links = []
    # 只在列表标题块内找，避免抓到面包屑、分页等
    for title_div in soup.find_all("div", class_="structItem-title"):
        a = title_div.find("a", href=True)
        if not a:
            continue
        href = (a.get("href") or "").strip()
        # 归一化，仅保留 /threads/<slug.id>/
        base_path = normalize_thread_path(href)
        if not base_path:
            continue
        links.append(to_abs_url(base_path))
    return links


def find_batch_dirs(parent: str, prefix: str, start: int, end: int):
    """
    在 parent 下查找所有以 prefix + <start..end> 开头的子目录。
    支持随机后缀：如 "cracking_forum_urls_level_1_batch_3_abcxyz"
    返回字典: {批次号: [目录绝对路径, ...], ...}
    """
    out = {}
    if not os.path.isdir(parent):
        return out
    for name in os.listdir(parent):
        path = os.path.join(parent, name)
        if not os.path.isdir(path):
            continue
        # 逐个批次匹配 startswith
        for n in range(start, end + 1):
            key = f"{prefix}{n}"
            if name.startswith(key):
                out.setdefault(n, []).append(path)
                break
    return out


def main():
    os.makedirs(SITE_OUT_DIR, exist_ok=True)

    rows = []   # dict(batch, dir, json, page, link, file_order, item_index)
    total_json = 0
    total_found = 0

    batch_dirs = find_batch_dirs(PARENT_DIR, BATCH_PREFIX, BATCH_START, BATCH_END)
    if not batch_dirs:
        print(f"[WARN] 在 {PARENT_DIR} 下未找到以 {BATCH_PREFIX}<N> 开头、N∈[{BATCH_START},{BATCH_END}] 的目录。")
    else:
        print(f"[INFO] 匹配到批次目录：")
        for n in sorted(batch_dirs):
            for d in sorted(batch_dirs[n]):
                print(f"  - batch {n}: {os.path.basename(d)}")

    # 遍历匹配到的批次目录
    for batch_no in sorted(batch_dirs.keys()):
        for batch_dir in sorted(batch_dirs[batch_no]):
            files = list(iter_json_files(batch_dir, recursive=RECURSIVE))
            files.sort(key=natural_key_for_path)
            total_json += len(files)

            batch_found = 0
            for file_order, jp in enumerate(files):
                try:
                    with open(jp, "r", encoding=ENCODING) as f:
                        rec = json.load(f)
                except Exception as e:
                    print(f"[ERR] 读取失败: {jp}: {e}")
                    continue

                page_url = pick_page_url(rec)
                page_num = extract_page_from_url(page_url)
                content = rec.get("content", "")

                try:
                    links = extract_thread_links_from_content(content)
                except Exception as e:
                    print(f"[ERR] 解析失败: {jp}: {e}")
                    continue

                for item_index, link in enumerate(links):
                    rows.append({
                        "batch": batch_no,
                        "dir": os.path.basename(batch_dir),
                        "json": os.path.basename(jp),
                        "page": page_num,
                        "link": link,
                        "file_order": file_order,
                        "item_index": item_index,
                    })
                batch_found += len(links)

            total_found += batch_found
            print(f"[OK] {os.path.basename(batch_dir)}: {len(files)} 个 JSON，提取 {batch_found} 条线程链接")

    # 排序并去重
    rows.sort(key=lambda r: (r["batch"], r["page"], r["file_order"], r["item_index"]))

    seen = set()
    uniq_rows = []
    for r in rows:
        if r["link"] not in seen:
            seen.add(r["link"])
            uniq_rows.append(r)

    # 输出 TXT
    with open(OUT_TXT, "w", encoding=ENCODING, newline="\n") as w:
        for r in uniq_rows:
            w.write(r["link"] + "\n")

    # 输出 CSV（包含 batch 与 目录名）
    with open(OUT_CSV, "w", encoding=CSV_ENCODING, newline="") as cf:
        writer = csv.writer(cf)
        writer.writerow(["batch", "dir", "json", "page", "link"])
        for r in uniq_rows:
            writer.writerow([r["batch"], r["dir"], r["json"], r["page"], r["link"]])

    # —— 控制台打印结果
    print("\n===== 提取结果（去重后）=====")
    for r in uniq_rows:
        print(r["link"])

    print("\n===== 统计 =====")
    print(f"批次范围         : {BATCH_START} ~ {BATCH_END}")
    print(f"批次目录前缀     : {BATCH_PREFIX}（允许随机后缀）")
    print(f"匹配到的批次数   : {len(batch_dirs)}")
    print(f"共扫描 JSON 文件 : {total_json}")
    print(f"提取原始链接数   : {total_found}")
    print(f"去重后链接数     : {len(uniq_rows)}")
    print(f"输出 TXT         : {OUT_TXT}")
    print(f"输出 CSV         : {OUT_CSV}")
    print("（CSV 列：batch, dir, json, page, link）")


if __name__ == "__main__":
    main()
